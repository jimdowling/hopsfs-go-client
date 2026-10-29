package rpc

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"

	hdfs "github.com/colinmarc/hdfs/v2/internal/protocol/hadoop_hdfs"
	"github.com/stretchr/testify/require"
)

func TestCloseInterruptsRPCAndPreventsReconnect(t *testing.T) {
	peer := &pipeNamenode{}
	conn, err := NewNamenodeConnection(NamenodeConnectionOptions{
		Addresses: []string{"test:1"}, User: "test", DialFunc: peer.dial, RequestTimeout: -1,
	})
	require.NoError(t, err)
	t.Cleanup(peer.hangUp)
	done := make(chan error, 1)
	go func() {
		done <- conn.Execute("renewLease", &hdfs.RenewLeaseRequestProto{ClientName: &conn.ClientName}, &hdfs.RenewLeaseResponseProto{})
	}()
	require.Eventually(t, func() bool { return len(conn.reqLock) == 1 }, time.Second, time.Millisecond)
	require.NoError(t, conn.Close())
	select {
	case err := <-done:
		require.Error(t, err)
	case <-time.After(time.Second):
		t.Fatal("RPC did not stop after Close")
	}
	require.NoError(t, conn.Close())
	err = conn.Execute("renewLease", &hdfs.RenewLeaseRequestProto{}, &hdfs.RenewLeaseResponseProto{})
	require.ErrorIs(t, err, io.ErrClosedPipe)
	dials, _ := peer.counts()
	require.Equal(t, 1, dials)
}

func TestRequestTimeoutBoundsUnresponsivePeer(t *testing.T) {
	peer := &pipeNamenode{}
	conn, err := NewNamenodeConnection(NamenodeConnectionOptions{
		Addresses: []string{"test:1"}, User: "test", DialFunc: peer.dial, RequestTimeout: 20 * time.Millisecond,
	})
	require.NoError(t, err)
	t.Cleanup(peer.hangUp)
	t.Cleanup(func() { conn.Close() })
	err = conn.Execute("renewLease", &hdfs.RenewLeaseRequestProto{ClientName: &conn.ClientName}, &hdfs.RenewLeaseResponseProto{})
	require.Error(t, err)
}

func TestQueuedRPCHonorsContext(t *testing.T) {
	peer := &pipeNamenode{}
	conn, err := NewNamenodeConnection(NamenodeConnectionOptions{
		Addresses: []string{"test:1"}, User: "test", DialFunc: peer.dial,
	})
	require.NoError(t, err)
	t.Cleanup(peer.hangUp)
	t.Cleanup(func() { conn.Close() })
	conn.reqLock <- struct{}{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = conn.ExecuteContext(ctx, "renewLease", &hdfs.RenewLeaseRequestProto{}, &hdfs.RenewLeaseResponseProto{})
	require.ErrorIs(t, err, context.Canceled)
	<-conn.reqLock
}

func TestContextCancellationInterruptsRPCWithoutDeadline(t *testing.T) {
	peer := &pipeNamenode{}
	conn, err := NewNamenodeConnection(NamenodeConnectionOptions{Addresses: []string{"test:1"}, User: "test", DialFunc: peer.dial, RequestTimeout: -1})
	require.NoError(t, err)
	t.Cleanup(peer.hangUp)
	t.Cleanup(func() { conn.Close() })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- conn.ExecuteContext(ctx, "renewLease", &hdfs.RenewLeaseRequestProto{ClientName: &conn.ClientName}, &hdfs.RenewLeaseResponseProto{})
	}()
	require.Eventually(t, func() bool { return len(conn.reqLock) == 1 }, time.Second, time.Millisecond)
	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("RPC did not stop after context cancellation")
	}
}

func TestEpochUpdatesAreConcurrentAndScopedToEachClient(t *testing.T) {
	first, second := &NamenodeConnection{}, &NamenodeConnection{}
	var workers sync.WaitGroup
	for _, conn := range []*NamenodeConnection{first, second} {
		conn := conn
		workers.Add(2)
		go func() {
			defer workers.Done()
			for i := 0; i < 1000; i++ {
				conn.SetEpoch(time.Now(), 12000)
			}
		}()
		go func() {
			defer workers.Done()
			for i := 0; i < 1000; i++ {
				conn.epochSec()
			}
		}()
	}
	workers.Wait()
	first.SetEpoch(time.Now(), 12000)
	second.SetEpoch(time.Now(), 24000)
	require.Equal(t, int64(12), first.epochSec())
	require.Equal(t, int64(24), second.epochSec())
}
