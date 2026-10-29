package transfer

import (
	"errors"
	"io"
	"net"
	"syscall"
	"testing"
	"time"

	hdfs "github.com/colinmarc/hdfs/v2/internal/protocol/hadoop_hdfs"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestFailedWriteStopsWorkersAndRetainsError(t *testing.T) {
	client, server := net.Pipe()
	server.Close()
	stream := newBlockWriteStream(client, 0)
	_, writeErr := stream.Write(make([]byte, outboundPacketSize))
	require.Error(t, writeErr)
	closeErr := stream.finish()
	require.Error(t, closeErr)
	require.Equal(t, closeErr, stream.finish())
	select {
	case <-stream.acksDone:
	case <-time.After(time.Second):
		t.Fatal("acknowledgement worker did not stop")
	}
	select {
	case <-stream.heartbeatsDone:
	case <-time.After(time.Second):
		t.Fatal("heartbeat worker did not stop")
	}
}

func TestAcknowledgementFailureIsVisibleBeforeClose(t *testing.T) {
	client, server := net.Pipe()
	stream := newBlockWriteStream(client, 0)
	t.Cleanup(func() { stream.abort(io.ErrClosedPipe); server.Close() })
	serverDone := make(chan error, 1)
	go func() {
		packet := make([]byte, outboundPacketSize+1024)
		if _, err := server.Read(packet); err != nil {
			serverDone <- err
			return
		}
		ack, err := makePrefixedMessage(&hdfs.PipelineAckProto{
			Seqno: proto.Int64(1), Reply: []hdfs.Status{hdfs.Status_ERROR_BLK_STORAGE_FULL},
		})
		if err == nil {
			_, err = server.Write(ack)
		}
		serverDone <- err
	}()
	_, err := stream.Write(make([]byte, outboundPacketSize))
	require.NoError(t, err)
	require.NoError(t, <-serverDone)
	require.Eventually(t, func() bool { return errors.Is(stream.getAckError(), syscall.ENOSPC) }, time.Second, time.Millisecond)
	_, err = stream.Write([]byte("more"))
	require.ErrorIs(t, err, syscall.ENOSPC)
	require.ErrorIs(t, stream.finish(), syscall.ENOSPC)
}
