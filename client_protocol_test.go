package hdfs

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	hadoop "github.com/colinmarc/hdfs/v2/internal/protocol/hadoop_common"
	hdfs "github.com/colinmarc/hdfs/v2/internal/protocol/hadoop_hdfs"
	"github.com/colinmarc/hdfs/v2/internal/rpc"
	"github.com/colinmarc/hdfs/v2/internal/transfer"
	"google.golang.org/protobuf/proto"
)

func newProtocolDialer(t *testing.T, handler func(string, []byte) proto.Message) dialContext {
	t.Helper()
	return func(context.Context, string, string) (net.Conn, error) {
		c, s := net.Pipe()
		go func() {
			defer s.Close()
			var magic [7]byte
			if _, err := io.ReadFull(s, magic[:]); err != nil {
				return
			}
			first := true
			for {
				var size uint32
				if err := binary.Read(s, binary.BigEndian, &size); err != nil {
					return
				}
				body := make([]byte, size)
				if _, err := io.ReadFull(s, body); err != nil {
					return
				}
				next := func() []byte {
					l, n := binary.Uvarint(body)
					part := body[n : n+int(l)]
					body = body[n+int(l):]
					return part
				}
				if first {
					first = false
					continue
				}
				header := &hadoop.RpcRequestHeaderProto{}
				if err := proto.Unmarshal(next(), header); err != nil {
					return
				}
				method := &hadoop.RequestHeaderProto{}
				if err := proto.Unmarshal(next(), method); err != nil {
					return
				}
				resp := handler(method.GetMethodName(), next())
				rh := &hadoop.RpcResponseHeaderProto{CallId: proto.Uint32(uint32(header.GetCallId())), Status: hadoop.RpcResponseHeaderProto_SUCCESS.Enum()}
				var frame bytes.Buffer
				for _, m := range []proto.Message{rh, resp} {
					b, err := proto.Marshal(m)
					if err != nil {
						t.Error(err)
						return
					}
					var v [10]byte
					n := binary.PutUvarint(v[:], uint64(len(b)))
					frame.Write(v[:n])
					frame.Write(b)
				}
				out := make([]byte, 4)
				binary.BigEndian.PutUint32(out, uint32(frame.Len()))
				out = append(out, frame.Bytes()...)
				if _, err := s.Write(out); err != nil {
					return
				}
			}
		}()
		return c, nil
	}
}

func newProtocolClient(t *testing.T, handler func(string, []byte) proto.Message) *Client {
	t.Helper()
	dial := newProtocolDialer(t, handler)
	nn, err := rpc.NewNamenodeConnection(rpc.NamenodeConnectionOptions{Addresses: []string{"fake:1"}, User: "review", DialFunc: dial})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { nn.Close() })
	return &Client{namenode: nn, leaderNamenode: nn, defaults: &hdfs.FsServerDefaultsProto{EncryptDataTransfer: proto.Bool(false)}}
}

func TestProtocolAppendGenerationStampDoesNotLeakToNextBlock(t *testing.T) {
	var previousGS, lastGS uint64
	block := &hdfs.LocatedBlockProto{B: &hdfs.ExtendedBlockProto{PoolId: proto.String("bp"), BlockId: proto.Uint64(2), GenerationStamp: proto.Uint64(200), NumBytes: proto.Uint64(0)}, Offset: proto.Uint64(8), Corrupt: proto.Bool(false), BlockToken: &hadoop.TokenProto{Identifier: []byte{}, Password: []byte{}, Kind: proto.String(""), Service: proto.String("")}}
	c := newProtocolClient(t, func(method string, raw []byte) proto.Message {
		switch method {
		case "addBlock":
			req := &hdfs.AddBlockRequestProto{}
			proto.Unmarshal(raw, req)
			previousGS = req.GetPrevious().GetGenerationStamp()
			return &hdfs.AddBlockResponseProto{Block: block}
		case "complete":
			req := &hdfs.CompleteRequestProto{}
			proto.Unmarshal(raw, req)
			lastGS = req.GetLast().GetGenerationStamp()
			return &hdfs.CompleteResponseProto{Result: proto.Bool(true)}
		default:
			return &hdfs.RenewLeaseResponseProto{}
		}
	})
	f := &FileWriter{client: c, name: "file", newGS: 101, blockWriter: &transfer.BlockWriter{Block: &hdfs.LocatedBlockProto{B: &hdfs.ExtendedBlockProto{PoolId: proto.String("bp"), BlockId: proto.Uint64(1), GenerationStamp: proto.Uint64(100), NumBytes: proto.Uint64(8)}}}}
	if err := f.startNewBlock(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if previousGS != 101 {
		t.Errorf("addBlock sent old generation %d instead of 101", previousGS)
	}
	if lastGS != 200 {
		t.Errorf("complete sent previous block's bumped generation %d for new block at generation 200", lastGS)
	}
}

type protocolWriteConn struct {
	bytes.Buffer
	writes int
	closed bool
}

func (c *protocolWriteConn) Write(b []byte) (int, error) {
	c.writes++
	if c.writes > 1 {
		return 0, errors.New("injected write failure")
	}
	return len(b), nil
}
func (c *protocolWriteConn) Close() error                     { c.closed = true; return nil }
func (c *protocolWriteConn) LocalAddr() net.Addr              { return &net.TCPAddr{} }
func (c *protocolWriteConn) RemoteAddr() net.Addr             { return &net.TCPAddr{} }
func (c *protocolWriteConn) SetDeadline(time.Time) error      { return nil }
func (c *protocolWriteConn) SetReadDeadline(time.Time) error  { return nil }
func (c *protocolWriteConn) SetWriteDeadline(time.Time) error { return nil }
func newProtocolWriter(t *testing.T) (*FileWriter, *protocolWriteConn) {
	t.Helper()
	conn := &protocolWriteConn{}
	response, _ := proto.Marshal(&hdfs.BlockOpResponseProto{Status: hdfs.Status_SUCCESS.Enum(), Message: proto.String("ok")})
	var prefix [10]byte
	n := binary.PutUvarint(prefix[:], uint64(len(response)))
	conn.Buffer.Write(prefix[:n])
	conn.Buffer.Write(response)
	block := &hdfs.LocatedBlockProto{B: &hdfs.ExtendedBlockProto{PoolId: proto.String("bp"), BlockId: proto.Uint64(1), GenerationStamp: proto.Uint64(1), NumBytes: proto.Uint64(0)}, StorageTypes: []hdfs.StorageTypeProto{hdfs.StorageTypeProto_DISK}, Locs: []*hdfs.DatanodeInfoProto{{Id: &hdfs.DatanodeIDProto{IpAddr: proto.String("127.0.0.1"), XferPort: proto.Uint32(1234)}}}}
	bw := &transfer.BlockWriter{Block: block, BlockSize: 1 << 20, DialFunc: func(context.Context, string, string) (net.Conn, error) { return conn, nil }}
	nn := &rpc.NamenodeConnection{}
	nn.AcquireLease()
	return &FileWriter{client: &Client{namenode: nn}, blockWriter: bw, leaseHeld: true}, conn
}

func TestProtocolFailedWriterCloseCleansUpPipeline(t *testing.T) {
	f, c := newProtocolWriter(t)
	_, err := f.Write(make([]byte, 65536))
	if err == nil {
		t.Fatal("expected write failure")
	}
	if err := f.Close(); err == nil {
		t.Fatal("expected close failure")
	}
	if !c.closed {
		t.Error("FileWriter.Close left failed pipeline connection open")
	}
	f.blockWriter.Close()
}

func TestProtocolCloseFailureIsTerminalAndReleasesLease(t *testing.T) {
	f, _ := newProtocolWriter(t)
	if _, err := f.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err == nil {
		t.Fatal("expected close failure")
	}
	if f.client.namenode.OpenWriters() != 0 {
		t.Error("terminal data-write failure in Close retains lease renewal")
	}
	if f.lastError == nil {
		t.Error("terminal Close error is not remembered for retries")
	}
}

func TestProtocolDBPromotionFailureIsTerminal(t *testing.T) {
	writer, conn := newProtocolWriter(t)
	writer.storeInDB = true
	n, err := writer.Write([]byte("prefix"))
	if err != nil || n != 6 {
		t.Fatalf("initial write: %d %v", n, err)
	}
	n, err = writer.Write(make([]byte, MaxSmallFileSize+1))
	if err == nil || n == MaxSmallFileSize+1 {
		t.Fatalf("promotion write: %d %v", n, err)
	}
	if writer.GetPos() != uint64(6+n) {
		t.Fatal("logical position includes unaccepted bytes")
	}
	if writer.lastError == nil || writer.client.namenode.OpenWriters() != 0 || !conn.closed {
		t.Fatal("failed promotion retained its pipeline or lease")
	}
	if writer.smallFileBuffer != nil {
		t.Fatal("failed promotion retained its DB buffer")
	}
	if closeErr := writer.Close(); closeErr != err {
		t.Fatalf("Close lost original error: %v vs %v", closeErr, err)
	}
}

func TestProtocolConcurrentDefaults(t *testing.T) {
	c := newProtocolClient(t, func(string, []byte) proto.Message {
		return &hdfs.GetServerDefaultsResponseProto{ServerDefaults: &hdfs.FsServerDefaultsProto{BlockSize: proto.Uint64(1024), BytesPerChecksum: proto.Uint32(512), WritePacketSize: proto.Uint32(65536), Replication: proto.Uint32(1), FileBufferSize: proto.Uint32(4096)}}
	})
	c.defaults = nil
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); <-start; c.fetchDefaults() }()
	}
	close(start)
	wg.Wait()
}
