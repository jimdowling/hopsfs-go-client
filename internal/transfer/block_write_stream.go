package transfer

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"sync"
	"syscall"
	"time"

	hdfs "github.com/colinmarc/hdfs/v2/internal/protocol/hadoop_hdfs"
	"google.golang.org/protobuf/proto"
)

const (
	outboundPacketSize = 65536
	outboundChunkSize  = 512
	maxPacketsInQueue  = 5
	heartbeatSeqno     = -1
	heartbeatInterval  = 30 * time.Second
)

// heartbeatPacket is sent every 30 seconds to keep the stream alive. It's
// always the same.
var heartbeatPacket []byte

func init() {
	b, err := proto.Marshal(&hdfs.PacketHeaderProto{
		OffsetInBlock:     proto.Int64(0),
		Seqno:             proto.Int64(heartbeatSeqno),
		LastPacketInBlock: proto.Bool(false),
		DataLen:           proto.Int32(0),
	})

	if err != nil {
		panic(err)
	}

	header := make([]byte, 6)
	binary.BigEndian.PutUint32(header, 4)
	binary.BigEndian.PutUint16(header[4:], uint16(len(b)))
	heartbeatPacket = append(header, b...)
}

// blockWriteStream writes data out to a datanode, and reads acks back.
type blockWriteStream struct {
	block *hdfs.LocatedBlockProto

	conn   io.ReadWriter
	buf    bytes.Buffer
	offset int64
	closed bool

	packets chan int
	seqno   int

	ackError        error
	errMu           sync.Mutex
	stopOnce        sync.Once
	packetsOnce     sync.Once
	heartbeatsDone  chan struct{}
	acksDone        chan struct{}
	lastPacketSeqno int

	heartbeats chan struct{}
	writeLock  sync.Mutex
}

type outboundPacket struct {
	seqno     int
	offset    int64
	last      bool
	checksums []byte
	data      []byte
}

type ackError struct {
	pipelineIndex int
	seqno         int
	status        hdfs.Status
}

func (ae ackError) Error() string {
	return fmt.Sprintf("Ack error from datanode: %s", ae.status.String())
}

var ErrInvalidSeqno = errors.New("invalid ack sequence number")

func newBlockWriteStream(conn io.ReadWriter, offset int64) *blockWriteStream {
	s := &blockWriteStream{
		conn:           conn,
		offset:         offset,
		seqno:          1,
		packets:        make(chan int, maxPacketsInQueue),
		acksDone:       make(chan struct{}),
		heartbeats:     make(chan struct{}),
		heartbeatsDone: make(chan struct{}),
	}

	// Send idle heartbeats every 30 seconds.
	go s.writeHeartbeats()

	// Ack packets in the background.
	go func() {
		s.ackPackets()
		close(s.acksDone)
	}()

	return s
}

func (s *blockWriteStream) Write(b []byte) (int, error) {
	if s.closed {
		return 0, io.ErrClosedPipe
	}

	if err := s.getAckError(); err != nil {
		return 0, err
	}

	accepted := 0
	for len(b) > 0 {
		count := outboundPacketSize - s.buf.Len()
		if count > len(b) {
			count = len(b)
		}
		s.buf.Write(b[:count])
		accepted += count
		b = b[count:]
		if err := s.flush(false); err != nil {
			s.setAckError(err)
			return accepted, err
		}
	}
	return accepted, nil
}

// finish flushes the rest of the buffered bytes, and then sends a final empty
// packet signifying the end of the block.
func (s *blockWriteStream) finish() error {
	if s.closed {
		return s.getAckError()
	}
	s.closed = true
	s.stopHeartbeats()
	if err := s.flush(true); err != nil {
		return s.abort(err)
	}
	lastPacket := outboundPacket{seqno: s.seqno, offset: s.offset, last: true}
	if err := s.queuePacket(lastPacket.seqno); err != nil {
		return s.abort(err)
	}
	s.writeLock.Lock()
	err := s.writePacket(lastPacket)
	s.writeLock.Unlock()
	if err != nil {
		return s.abort(err)
	}
	s.closePackets()
	<-s.acksDone
	if err := s.getAckError(); err != nil {
		return s.abort(err)
	}
	<-s.heartbeatsDone
	s.buf = bytes.Buffer{}
	return nil
}

func (s *blockWriteStream) stopHeartbeats() {
	s.stopOnce.Do(func() { close(s.heartbeats) })
}

func (s *blockWriteStream) closePackets() {
	s.packetsOnce.Do(func() { close(s.packets) })
}

func (s *blockWriteStream) abort(err error) error {
	s.closed = true
	s.setAckError(err)
	s.stopHeartbeats()
	if conn, ok := s.conn.(io.Closer); ok {
		conn.Close()
	}
	s.closePackets()
	<-s.acksDone
	<-s.heartbeatsDone
	s.buf = bytes.Buffer{}
	return s.getAckError()
}

func (s *blockWriteStream) queuePacket(seqno int) error {
	if err := s.getAckError(); err != nil {
		return err
	}
	select {
	case s.packets <- seqno:
		return nil
	case <-s.acksDone:
		if err := s.getAckError(); err != nil {
			return err
		}
		return io.ErrClosedPipe
	}
}

// flush sends complete packets, including a final partial packet when force is true.
func (s *blockWriteStream) flush(force bool) error {
	if err := s.getAckError(); err != nil {
		return err
	}
	s.writeLock.Lock()
	defer s.writeLock.Unlock()

	for s.buf.Len() > 0 && (force || s.buf.Len() >= outboundPacketSize) {
		packet := s.makePacket()
		if err := s.queuePacket(packet.seqno); err != nil {
			return err
		}
		s.offset += int64(len(packet.data))
		s.seqno++

		err := s.writePacket(packet)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *blockWriteStream) makePacket() outboundPacket {
	packetLength := outboundPacketSize
	if s.buf.Len() < outboundPacketSize {
		packetLength = s.buf.Len()
	}

	// If we're starting from a weird offset (usually because of an Append), HDFS
	// gets unhappy unless we first align to a chunk boundary with a small packet.
	// Otherwise it yells at us with "a partial chunk must be sent in an
	// individual packet" or just complains about a corrupted block.
	alignment := int(s.offset) % outboundChunkSize
	if alignment > 0 && packetLength > (outboundChunkSize-alignment) {
		packetLength = outboundChunkSize - alignment
	}

	numChunks := int(math.Ceil(float64(packetLength) / float64(outboundChunkSize)))
	packet := outboundPacket{
		seqno:     s.seqno,
		offset:    s.offset,
		last:      false,
		checksums: make([]byte, numChunks*4),
		data:      s.buf.Next(packetLength),
	}

	// Fill in the checksum for each chunk of data.
	tab := crc32.MakeTable(crc32.Castagnoli)
	for i := 0; i < numChunks; i++ {
		chunkOff := i * outboundChunkSize
		chunkEnd := chunkOff + outboundChunkSize
		if chunkEnd >= len(packet.data) {
			chunkEnd = len(packet.data)
		}

		checksum := crc32.Checksum(packet.data[chunkOff:chunkEnd], tab)
		binary.BigEndian.PutUint32(packet.checksums[i*4:], checksum)
	}

	return packet
}

// ackPackets is meant to run in the background, reading acks and setting
// ackError if one fails.
func (s *blockWriteStream) ackPackets() {
	reader := bufio.NewReader(s.conn)
	for expected := range s.packets {
		for {
			ack := &hdfs.PipelineAckProto{}
			if err := readPrefixedMessage(reader, ack); err != nil {
				s.setAckError(err)
				return
			}
			seqno := int(ack.GetSeqno())
			if len(ack.GetReply()) == 0 {
				s.setAckError(errInvalidResponse)
				return
			}
			for i, status := range ack.GetReply() {
				if status != hdfs.Status_SUCCESS {
					s.setAckError(ackError{status: status, seqno: seqno, pipelineIndex: i})
					return
				}
			}
			if seqno == heartbeatSeqno {
				continue
			}
			if seqno != expected {
				s.setAckError(ErrInvalidSeqno)
				return
			}
			break
		}
	}
}

func (s *blockWriteStream) setAckError(err error) {
	s.errMu.Lock()
	defer s.errMu.Unlock()
	if s.ackError == nil {
		s.ackError = err
	}
}

func (s *blockWriteStream) getAckError() error {
	s.errMu.Lock()
	defer s.errMu.Unlock()
	if ae, ok := s.ackError.(ackError); ok && ae.status == hdfs.Status_ERROR_BLK_STORAGE_FULL {
		return syscall.ENOSPC
	}
	return s.ackError
}

// A packet for the datanode:
// +-----------------------------------------------------------+
// |  uint32 length of the packet                              |
// +-----------------------------------------------------------+
// |  size of the PacketHeaderProto, uint16                    |
// +-----------------------------------------------------------+
// |  PacketHeaderProto                                        |
// +-----------------------------------------------------------+
// |  N checksums, 4 bytes each                                |
// +-----------------------------------------------------------+
// |  N chunks of payload data                                 |
// +-----------------------------------------------------------+
func (s *blockWriteStream) writePacket(p outboundPacket) error {
	headerInfo := &hdfs.PacketHeaderProto{
		OffsetInBlock:     proto.Int64(p.offset),
		Seqno:             proto.Int64(int64(p.seqno)),
		LastPacketInBlock: proto.Bool(p.last),
		DataLen:           proto.Int32(int32(len(p.data))),
	}

	// Don't ask me why this doesn't include the header proto...
	totalLength := len(p.data) + len(p.checksums) + 4

	infoBytes, err := proto.Marshal(headerInfo)
	if err != nil {
		return err
	}

	header := make([]byte, 6, 6+len(infoBytes)+len(p.checksums)+len(p.data))
	binary.BigEndian.PutUint32(header, uint32(totalLength))
	binary.BigEndian.PutUint16(header[4:], uint16(len(infoBytes)))
	header = append(header, infoBytes...)
	header = append(header, p.checksums...)
	header = append(header, p.data...)

	_, err = s.conn.Write(header)
	if err != nil {
		return err
	}

	return nil
}

func (s *blockWriteStream) writeHeartbeats() {
	defer close(s.heartbeatsDone)
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.writeLock.Lock()
			_, err := s.conn.Write(heartbeatPacket)
			s.writeLock.Unlock()
			if err != nil {
				s.setAckError(err)
				return
			}
		case <-s.heartbeats:
			return
		}
	}
}
