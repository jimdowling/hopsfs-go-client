package transfer

import (
	"bytes"
	"context"
	"io"
	"net"
	"testing"
	"time"

	hdfs "github.com/colinmarc/hdfs/v2/internal/protocol/hadoop_hdfs"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

type setupConn struct {
	bytes.Buffer
	deadline            time.Time
	closed              bool
	readWithoutDeadline bool
}

func (c *setupConn) Read(b []byte) (int, error) {
	if c.deadline.IsZero() {
		c.readWithoutDeadline = true
	}
	return c.Buffer.Read(b)
}
func (c *setupConn) Write(b []byte) (int, error)               { return len(b), nil }
func (c *setupConn) Close() error                              { c.closed = true; return nil }
func (c *setupConn) LocalAddr() net.Addr                       { return &net.TCPAddr{} }
func (c *setupConn) RemoteAddr() net.Addr                      { return &net.TCPAddr{} }
func (c *setupConn) SetDeadline(deadline time.Time) error      { c.deadline = deadline; return nil }
func (c *setupConn) SetReadDeadline(deadline time.Time) error  { return c.SetDeadline(deadline) }
func (c *setupConn) SetWriteDeadline(deadline time.Time) error { return c.SetDeadline(deadline) }

func TestDataNodeSetupHonorsDeadlineAndClosesFailedConnection(t *testing.T) {
	for _, mode := range []string{"read", "write", "checksum"} {
		t.Run(mode, func(t *testing.T) {
			conn := &setupConn{}
			dial := func(ctx context.Context, _, _ string) (net.Conn, error) {
				_, ok := ctx.Deadline()
				require.True(t, ok, "dial context has no operation deadline")
				return conn, nil
			}
			deadline := time.Now().Add(time.Second)
			block := locatedBlockForWrite(nil)
			var err error
			switch mode {
			case "read":
				reader := &BlockReader{Block: block, datanodes: newDatanodeFailover([]string{"local:1"}), DialFunc: dial}
				require.NoError(t, reader.SetDeadline(deadline))
				err = reader.connectNext()
			case "write":
				writer := &BlockWriter{Block: block, DialFunc: dial}
				require.NoError(t, writer.SetDeadline(deadline))
				err = writer.connectNext()
			case "checksum":
				reader := &ChecksumReader{Block: block, DialFunc: dial}
				require.NoError(t, reader.SetDeadline(deadline))
				_, err = reader.readChecksum("local:1")
			}
			require.Error(t, err)
			require.True(t, conn.closed, "failed setup leaked the connection")
			require.False(t, conn.readWithoutDeadline, "setup I/O has no deadline")
		})
	}
}

func TestChecksumResponseStatusAndCleanup(t *testing.T) {
	checksum := bytes.Repeat([]byte{7}, 16)
	for _, status := range []hdfs.Status{hdfs.Status_SUCCESS, hdfs.Status_ERROR_ACCESS_TOKEN} {
		conn := &setupConn{}
		packet, err := makePrefixedMessage(&hdfs.BlockOpResponseProto{
			Status: status.Enum(),
			ChecksumResponse: &hdfs.OpBlockChecksumResponseProto{
				BytesPerCrc: proto.Uint32(512), CrcPerBlock: proto.Uint64(1), BlockChecksum: checksum,
			},
		})
		require.NoError(t, err)
		conn.Buffer.Write(packet)
		reader := &ChecksumReader{Block: locatedBlockForWrite(nil), DialFunc: func(context.Context, string, string) (net.Conn, error) { return conn, nil }}
		actual, err := reader.readChecksum("local:1")
		if status == hdfs.Status_SUCCESS {
			require.NoError(t, err)
			require.Equal(t, checksum, actual)
		} else {
			require.Error(t, err)
			require.Nil(t, actual)
		}
		require.True(t, conn.closed)
	}
	reader := &ChecksumReader{Block: &hdfs.LocatedBlockProto{}}
	actual, err := reader.ReadChecksum()
	require.Error(t, err)
	require.Nil(t, actual)
}

func TestDataNodeAddressOverridesReturnErrors(t *testing.T) {
	t.Setenv(HOPSFS_CLOUD_DATANODE_HOSTNAME_OVERRIDE_ENV, "")
	t.Setenv(HOPSFS_CLOUD_DATANODE_PORT_OVERRIDE_ENV, "")
	dn := &hdfs.DatanodeIDProto{IpAddr: proto.String("2001:db8::1"), XferPort: proto.Uint32(50010)}
	address, err := getDatanodeAddress(dn, false)
	require.NoError(t, err)
	require.Equal(t, "[2001:db8::1]:50010", address)
	for _, port := range []string{"bad", "-1", "0", "65536"} {
		t.Setenv(HOPSFS_CLOUD_DATANODE_PORT_OVERRIDE_ENV, port)
		_, err = getDatanodeAddress(dn, false)
		require.Error(t, err)
	}
}

var _ io.ReadWriteCloser = (*setupConn)(nil)
