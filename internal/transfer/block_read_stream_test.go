package transfer

import (
	"bytes"
	"context"
	"encoding/binary"
	"hash/crc32"
	"io"
	"net"
	"testing"

	hdfs "github.com/colinmarc/hdfs/v2/internal/protocol/hadoop_hdfs"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func readPacket(t *testing.T, data []byte, chunkSize int, last bool, offsets ...int64) []byte {
	t.Helper()
	offset := int64(0)
	if len(offsets) > 0 {
		offset = offsets[0]
	}
	header, err := proto.Marshal(&hdfs.PacketHeaderProto{
		OffsetInBlock: proto.Int64(offset), Seqno: proto.Int64(1),
		LastPacketInBlock: proto.Bool(last), DataLen: proto.Int32(int32(len(data))),
	})
	require.NoError(t, err)
	checksums := make([]byte, (len(data)+chunkSize-1)/chunkSize*4)
	for off := 0; off < len(data); off += chunkSize {
		end := off + chunkSize
		if end > len(data) {
			end = len(data)
		}
		binary.BigEndian.PutUint32(checksums[off/chunkSize*4:], crc32.ChecksumIEEE(data[off:end]))
	}
	wire := make([]byte, 6)
	binary.BigEndian.PutUint32(wire, uint32(4+len(checksums)+len(data)))
	binary.BigEndian.PutUint16(wire[4:], uint16(len(header)))
	wire = append(wire, header...)
	wire = append(wire, checksums...)
	return append(wire, data...)
}

func TestBlockReaderRetriesCorruptChunkOnHealthyReplica(t *testing.T) {
	wire := readPacket(t, []byte("abcdefghijkl"), 4, false)
	wire[len(wire)-8] ^= 1
	block := locatedBlockForWrite(nil)
	block.B.NumBytes = proto.Uint64(12)
	reader := &BlockReader{
		ClientName: "test", Block: block,
		stream:    newBlockReadStream(bytes.NewReader(wire), 4, crc32.IEEETable, 4),
		datanodes: newDatanodeFailover([]string{"healthy:1"}),
	}
	serverDone := make(chan error, 1)
	healthyWire := readPacket(t, []byte("efghijkl"), 4, false, 4)
	reader.DialFunc = func(context.Context, string, string) (net.Conn, error) {
		client, server := net.Pipe()
		go func() {
			defer server.Close()
			var prefix [3]byte
			if _, err := io.ReadFull(server, prefix[:]); err != nil {
				serverDone <- err
				return
			}
			request := &hdfs.OpReadBlockProto{}
			if err := readPrefixedMessage(server, request); err != nil {
				serverDone <- err
				return
			}
			if request.GetOffset() != 4 {
				t.Errorf("retry offset=%d, expected 4", request.GetOffset())
			}
			response, err := makePrefixedMessage(&hdfs.BlockOpResponseProto{
				Status: hdfs.Status_SUCCESS.Enum(),
				ReadOpChecksumInfo: &hdfs.ReadOpChecksumInfoProto{
					Checksum:    &hdfs.ChecksumProto{Type: hdfs.ChecksumTypeProto_CHECKSUM_CRC32.Enum(), BytesPerChecksum: proto.Uint32(4)},
					ChunkOffset: proto.Uint64(4),
				},
			})
			if err == nil {
				_, err = server.Write(response)
			}
			if err == nil {
				_, err = server.Write(healthyWire)
			}
			serverDone <- err
		}()
		return client, nil
	}
	defer reader.Close()
	data, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, "abcdefghijkl", string(data))
	require.NoError(t, <-serverDone)
}

func TestBlockReadStreamReturnsOnlyVerifiedChunks(t *testing.T) {
	for _, corrupt := range []int{0, 4, 8} {
		wire := readPacket(t, []byte("abcdefghijkl"), 4, false)
		wire[len(wire)-12+corrupt] ^= 1
		stream := newBlockReadStream(bytes.NewReader(wire), 4, crc32.IEEETable, 4)
		out := make([]byte, 12)
		n, err := stream.Read(out)
		require.ErrorIs(t, err, errInvalidChecksum)
		require.Equal(t, corrupt, n)
		require.Equal(t, "abcdefghijkl"[:corrupt], string(out[:n]))
	}
}

func TestBlockReadStreamDoesNotReturnUnverifiedTruncatedChunk(t *testing.T) {
	wire := readPacket(t, []byte("abcdefgh"), 4, false)
	stream := newBlockReadStream(bytes.NewReader(wire[:len(wire)-1]), 4, crc32.IEEETable, 4)
	out := make([]byte, 8)
	n, err := stream.Read(out)
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
	require.Equal(t, 4, n)
	require.Equal(t, "abcd", string(out[:n]))
}

func TestBlockReadStreamDrainsBufferedFinalChunk(t *testing.T) {
	for _, markedLast := range []bool{false, true} {
		wire := readPacket(t, []byte("abcdefgh"), 4, markedLast)
		if !markedLast {
			wire = append(wire, readPacket(t, nil, 4, true)...)
		}
		stream := newBlockReadStream(bytes.NewReader(wire), 4, crc32.IEEETable, 4)
		var out []byte
		for {
			var b [1]byte
			n, err := stream.Read(b[:])
			out = append(out, b[:n]...)
			if err != nil {
				require.ErrorIs(t, err, io.EOF)
				break
			}
		}
		require.Equal(t, "abcdefgh", string(out))
	}
}

func TestBlockReaderDoesNotAdvancePastCorruptChunk(t *testing.T) {
	wire := readPacket(t, []byte("good"), 4, false)
	wire[len(wire)-1] = 'X'
	reader := &BlockReader{
		Block:     &hdfs.LocatedBlockProto{B: &hdfs.ExtendedBlockProto{NumBytes: proto.Uint64(4)}},
		stream:    newBlockReadStream(bytes.NewReader(wire), 4, crc32.IEEETable, 4),
		datanodes: newDatanodeFailover(nil),
	}
	n, err := reader.Read(make([]byte, 4))
	require.ErrorIs(t, err, errInvalidChecksum)
	require.Zero(t, n)
	require.Zero(t, reader.Offset)
}

func TestBlockReaderRejectsEOFBeforeDeclaredBlockEnd(t *testing.T) {
	reader := &BlockReader{
		Block:     &hdfs.LocatedBlockProto{B: &hdfs.ExtendedBlockProto{NumBytes: proto.Uint64(4)}},
		stream:    newBlockReadStream(bytes.NewReader(nil), 4, crc32.IEEETable, 4),
		datanodes: newDatanodeFailover(nil),
	}
	n, err := reader.Read(make([]byte, 4))
	require.Zero(t, n)
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
}
