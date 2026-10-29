package hdfs

import (
	"bufio"
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"net"
	"sync/atomic"
	"testing"

	hadoop "github.com/colinmarc/hdfs/v2/internal/protocol/hadoop_common"
	hdfs "github.com/colinmarc/hdfs/v2/internal/protocol/hadoop_hdfs"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

type capturedBlock struct {
	id   uint64
	data []byte
	err  error
}

func writeDelimited(w io.Writer, message proto.Message) error {
	data, err := proto.Marshal(message)
	if err != nil {
		return err
	}
	var prefix [10]byte
	n := binary.PutUvarint(prefix[:], uint64(len(data)))
	_, err = w.Write(append(prefix[:n], data...))
	return err
}

func serveWritePipeline(conn net.Conn) (result capturedBlock) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	var operation [3]byte
	if _, result.err = io.ReadFull(reader, operation[:]); result.err != nil {
		return
	}
	length, err := binary.ReadUvarint(reader)
	if err != nil || length > 1<<20 {
		result.err = fmt.Errorf("invalid write operation length: %d (%v)", length, err)
		return
	}
	requestBytes := make([]byte, int(length))
	if _, result.err = io.ReadFull(reader, requestBytes); result.err != nil {
		return
	}
	request := &hdfs.OpWriteBlockProto{}
	if result.err = proto.Unmarshal(requestBytes, request); result.err != nil {
		return
	}
	result.id = request.GetHeader().GetBaseHeader().GetBlock().GetBlockId()
	if result.err = writeDelimited(conn, &hdfs.BlockOpResponseProto{Status: hdfs.Status_SUCCESS.Enum()}); result.err != nil {
		return
	}
	for {
		var prefix [6]byte
		if _, result.err = io.ReadFull(reader, prefix[:]); result.err != nil {
			return
		}
		payloadLength := int(binary.BigEndian.Uint32(prefix[:4])) - 4
		headerLength := int(binary.BigEndian.Uint16(prefix[4:]))
		if payloadLength < 0 || payloadLength > 1<<20 {
			result.err = fmt.Errorf("invalid packet length: %d", payloadLength)
			return
		}
		headerBytes := make([]byte, headerLength)
		if _, result.err = io.ReadFull(reader, headerBytes); result.err != nil {
			return
		}
		header := &hdfs.PacketHeaderProto{}
		if result.err = proto.Unmarshal(headerBytes, header); result.err != nil {
			return
		}
		payload := make([]byte, payloadLength)
		if _, result.err = io.ReadFull(reader, payload); result.err != nil {
			return
		}
		dataLength := int(header.GetDataLen())
		checksumLength := (dataLength + 511) / 512 * 4
		if dataLength < 0 || checksumLength+dataLength != payloadLength {
			result.err = fmt.Errorf("invalid checksum layout")
			return
		}
		data := payload[checksumLength:]
		for offset := 0; offset < dataLength; offset += 512 {
			end := offset + 512
			if end > dataLength {
				end = dataLength
			}
			if binary.BigEndian.Uint32(payload[offset/512*4:]) != crc32.Checksum(data[offset:end], crc32.MakeTable(crc32.Castagnoli)) {
				result.err = fmt.Errorf("invalid packet checksum")
				return
			}
		}
		result.data = append(result.data, data...)
		if result.err = writeDelimited(conn, &hdfs.PipelineAckProto{Seqno: header.Seqno, Reply: []hdfs.Status{hdfs.Status_SUCCESS}}); result.err != nil {
			return
		}
		if header.GetLastPacketInBlock() {
			return
		}
	}
}

func TestProtocolEncryptedDBPromotionAcrossBlocks(t *testing.T) {
	var blocks atomic.Uint64
	client := newProtocolClient(t, func(method string, raw []byte) proto.Message {
		switch method {
		case "addBlock":
			id := blocks.Add(1)
			return &hdfs.AddBlockResponseProto{Block: &hdfs.LocatedBlockProto{
				B:      &hdfs.ExtendedBlockProto{PoolId: proto.String("bp"), BlockId: proto.Uint64(id), GenerationStamp: proto.Uint64(1000 + id), NumBytes: proto.Uint64(0)},
				Offset: proto.Uint64((id - 1) * 32768), Corrupt: proto.Bool(false), StorageTypes: []hdfs.StorageTypeProto{hdfs.StorageTypeProto_DISK},
				Locs:       []*hdfs.DatanodeInfoProto{{Id: &hdfs.DatanodeIDProto{IpAddr: proto.String("127.0.0.1"), XferPort: proto.Uint32(1234), HostName: proto.String("localhost"), DatanodeUuid: proto.String("test"), InfoPort: proto.Uint32(0), IpcPort: proto.Uint32(0)}}},
				BlockToken: &hadoop.TokenProto{Identifier: []byte{}, Password: []byte{}, Kind: proto.String(""), Service: proto.String("")},
			}}
		case "complete":
			return &hdfs.CompleteResponseProto{Result: proto.Bool(true)}
		default:
			t.Errorf("unexpected method %s", method)
			return &hdfs.RenewLeaseResponseProto{}
		}
	})
	results := make(chan capturedBlock, 10)
	client.options.DatanodeDialFunc = func(context.Context, string, string) (net.Conn, error) {
		local, remote := net.Pipe()
		go func() { results <- serveWritePipeline(remote) }()
		return local, nil
	}
	key, iv := []byte("1234567890123456"), make([]byte, aes.BlockSize)
	writer := &FileWriter{client: client, name: "file", storeInDB: true, blockSize: 32768, enc: &transparentEncryptionInfo{key: key, iv: iv}}
	_, err := writer.Write([]byte("abc"))
	require.NoError(t, err)
	body := bytes.Repeat([]byte("x"), 64<<10+1234)
	n, err := writer.Write(body)
	require.NoError(t, err)
	require.Len(t, body, n)
	require.EqualValues(t, len(body)+3, writer.GetPos())
	require.Nil(t, writer.smallFileBuffer)
	require.NoError(t, writer.Close())
	byID := make(map[uint64][]byte)
	for i := uint64(0); i < blocks.Load(); i++ {
		result := <-results
		require.NoError(t, result.err)
		byID[result.id] = result.data
	}
	var ciphertext []byte
	for i := uint64(1); i <= blocks.Load(); i++ {
		ciphertext = append(ciphertext, byID[i]...)
	}
	block, err := aes.NewCipher(key)
	require.NoError(t, err)
	cipher.NewCTR(block, iv).XORKeyStream(ciphertext, ciphertext)
	require.Equal(t, append([]byte("abc"), body...), ciphertext)
	require.NoError(t, writer.Close())
	_, err = writer.Write([]byte("after close"))
	require.ErrorIs(t, err, io.ErrClosedPipe)
}
