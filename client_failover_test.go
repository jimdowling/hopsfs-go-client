package hdfs

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	hdfs "github.com/colinmarc/hdfs/v2/internal/protocol/hadoop_hdfs"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestProtocolClientToleratesUnavailableFirstAddress(t *testing.T) {
	dial := newProtocolDialer(t, func(method string, raw []byte) proto.Message {
		if method == "getNNEpochMS" {
			return &hdfs.GetEpochMSResponseProto{Epoch: proto.Int64(time.Now().UnixMilli())}
		}
		if method == "getServerDefaults" {
			return protocolDefaults()
		}
		return &hdfs.ActiveNamenodeListResponseProto{}
	})
	client, err := NewClient(ClientOptions{Addresses: []string{"bad:1", "alive:1"}, User: "test", NamenodeDialFunc: func(ctx context.Context, network, address string) (net.Conn, error) {
		if address == "bad:1" {
			return nil, errors.New("unreachable")
		}
		return dial(ctx, network, address)
	}})
	require.NoError(t, err)
	require.NoError(t, client.Close())
}

type failingAddressConn struct {
	net.Conn
	failed func() bool
}

func (c *failingAddressConn) Write(data []byte) (int, error) {
	if c.failed() {
		return 0, io.ErrClosedPipe
	}
	return c.Conn.Write(data)
}
func (c *failingAddressConn) Read(data []byte) (int, error) {
	if c.failed() {
		return 0, io.ErrClosedPipe
	}
	return c.Conn.Read(data)
}

func TestProtocolDiscoveryRetainsAlternateNameNodes(t *testing.T) {
	dial := newProtocolDialer(t, func(method string, raw []byte) proto.Message {
		switch method {
		case "getServerDefaults":
			return protocolDefaults()
		case "getNNEpochMS":
			return &hdfs.GetEpochMSResponseProto{Epoch: proto.Int64(time.Now().UnixMilli())}
		case "getActiveNamenodesForClient":
			return &hdfs.ActiveNamenodeListResponseProto{Namenodes: []*hdfs.ActiveNodeProto{
				{RpcIpAddress: proto.String("127.0.0.1"), RpcPort: proto.Int32(10001)},
				{RpcIpAddress: proto.String("127.0.0.1"), RpcPort: proto.Int32(10002)},
			}}
		case "getFileInfo":
			return &hdfs.GetFileInfoResponseProto{Fs: protocolStatus("file", hdfs.HdfsFileStatusProto_IS_FILE, 0)}
		default:
			t.Errorf("unexpected method %s", method)
			return &hdfs.RenewLeaseResponseProto{}
		}
	})
	var failed atomic.Value
	failed.Store("")
	var mutex sync.Mutex
	var addresses []string
	client, err := NewClient(ClientOptions{Addresses: []string{"bootstrap:1"}, User: "test", NamenodeDialFunc: func(ctx context.Context, network, address string) (net.Conn, error) {
		mutex.Lock()
		addresses = append(addresses, address)
		mutex.Unlock()
		conn, err := dial(ctx, network, address)
		if err != nil {
			return nil, err
		}
		return &failingAddressConn{Conn: conn, failed: func() bool { return failed.Load().(string) == address }}, nil
	}})
	require.NoError(t, err)
	defer client.Close()
	mutex.Lock()
	selected := client.options.Addresses[0]
	mutex.Unlock()
	failed.Store(selected)
	_, err = client.Stat("/file")
	require.NoError(t, err)
	require.Contains(t, client.options.Addresses, "127.0.0.1:10001")
	require.Contains(t, client.options.Addresses, "127.0.0.1:10002")
}

func protocolDefaults() *hdfs.GetServerDefaultsResponseProto {
	return &hdfs.GetServerDefaultsResponseProto{ServerDefaults: &hdfs.FsServerDefaultsProto{
		BlockSize: proto.Uint64(1 << 20), BytesPerChecksum: proto.Uint32(512), WritePacketSize: proto.Uint32(65536), Replication: proto.Uint32(1), FileBufferSize: proto.Uint32(4096),
	}}
}

func TestProtocolClientCompletesFirstRPCBeforeOpeningLeader(t *testing.T) {
	firstRPC := make(chan struct{})
	var ready sync.Once
	dial := newProtocolDialer(t, func(method string, raw []byte) proto.Message {
		ready.Do(func() { close(firstRPC) })
		switch method {
		case "getServerDefaults":
			return protocolDefaults()
		case "getNNEpochMS":
			return &hdfs.GetEpochMSResponseProto{Epoch: proto.Int64(time.Now().UnixMilli())}
		default:
			return &hdfs.ActiveNamenodeListResponseProto{}
		}
	})
	var calls atomic.Int32
	client, err := NewClient(ClientOptions{Addresses: []string{"first:1", "second:1"}, User: "test", NamenodeDialTimeout: 100 * time.Millisecond, NamenodeDialFunc: func(ctx context.Context, network, address string) (net.Conn, error) {
		if calls.Add(1) == 2 {
			select {
			case <-firstRPC:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return dial(ctx, network, address)
	}})
	require.NoError(t, err)
	defer client.Close()
	require.EqualValues(t, 2, calls.Load())
	require.NotNil(t, client.defaults)
}

func TestProtocolSingleNameNodeSharesConnection(t *testing.T) {
	var calls atomic.Int32
	dial := newProtocolDialer(t, func(method string, raw []byte) proto.Message {
		if method == "getNNEpochMS" {
			return &hdfs.GetEpochMSResponseProto{Epoch: proto.Int64(time.Now().UnixMilli())}
		}
		return &hdfs.ActiveNamenodeListResponseProto{}
	})
	client, err := NewClient(ClientOptions{Addresses: []string{"single:1"}, User: "test", NamenodeDialFunc: func(ctx context.Context, network, address string) (net.Conn, error) {
		calls.Add(1)
		return dial(ctx, network, address)
	}})
	require.NoError(t, err)
	require.Same(t, client.namenode, client.leaderNamenode)
	require.EqualValues(t, 1, calls.Load())
	require.NoError(t, client.Close())
	require.NoError(t, client.Close())
}

func TestProtocolMetadataMutationsAvoidPreflightRPCs(t *testing.T) {
	var stats atomic.Int32
	client := newProtocolClient(t, func(method string, raw []byte) proto.Message {
		switch method {
		case "getFileInfo":
			stats.Add(1)
			return &hdfs.GetFileInfoResponseProto{}
		case "create":
			return &hdfs.CreateResponseProto{Fs: protocolStatus("file", hdfs.HdfsFileStatusProto_IS_FILE, 0)}
		case "complete":
			return &hdfs.CompleteResponseProto{Result: proto.Bool(true)}
		case "delete":
			return &hdfs.DeleteResponseProto{Result: proto.Bool(true)}
		case "rename2":
			return &hdfs.Rename2ResponseProto{}
		case "setReplication":
			return &hdfs.SetReplicationResponseProto{Result: proto.Bool(true)}
		default:
			t.Errorf("unexpected method %s", method)
			return &hdfs.RenewLeaseResponseProto{}
		}
	})
	client.defaults.BlockSize = proto.Uint64(1024)
	client.defaults.Replication = proto.Uint32(1)
	writer, err := client.Create("/file")
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	require.NoError(t, client.Remove("/file"))
	require.NoError(t, client.Rename("/old", "/new"))
	ok, err := client.SetReplication("/file", 2)
	require.NoError(t, err)
	require.True(t, ok)
	require.Zero(t, stats.Load())
}
