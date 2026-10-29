package hdfs

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	hadoop "github.com/colinmarc/hdfs/v2/internal/protocol/hadoop_common"
	hdfs "github.com/colinmarc/hdfs/v2/internal/protocol/hadoop_hdfs"
	"github.com/colinmarc/hdfs/v2/internal/transfer"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestKMSResponseLimit(t *testing.T) {
	client := &Client{options: ClientOptions{KMSMaxResponseLength: 16}}
	_, err := client.readKMSBody(strings.NewReader(strings.Repeat("x", 100)))
	require.Error(t, err)
	data, err := client.readKMSBody(strings.NewReader("valid"))
	require.NoError(t, err)
	require.Equal(t, "valid", string(data))
}

func TestEncryptionKeyUnknownResponseRetriesOnce(t *testing.T) {
	var fetches, dials atomic.Int32
	client := newProtocolClient(t, func(method string, raw []byte) proto.Message {
		if method != "getDataEncryptionKey" {
			t.Errorf("unexpected method %s", method)
		}
		id := fetches.Add(1)
		return &hdfs.GetDataEncryptionKeyResponseProto{DataEncryptionKey: &hdfs.DataEncryptionKeyProto{
			KeyId: proto.Uint32(uint32(id)), BlockPoolId: proto.String("bp"), Nonce: []byte{1}, EncryptionKey: []byte("1234567890123456"), ExpiryDate: proto.Uint64(uint64(time.Now().Add(time.Hour).UnixMilli())),
		}}
	})
	client.options.DataTransferProtection = "privacy"
	dial := func(context.Context, string, string) (net.Conn, error) {
		dials.Add(1)
		local, remote := net.Pipe()
		go func() {
			defer remote.Close()
			var first [7]byte
			if _, err := io.ReadFull(remote, first[:]); err != nil {
				return
			}
			writeDelimited(remote, &hdfs.DataTransferEncryptorMessageProto{Status: hdfs.DataTransferEncryptorMessageProto_ERROR_UNKNOWN_KEY.Enum()})
		}()
		return local, nil
	}
	wrapped, err := client.wrapDatanodeDial(dial, &hadoop.TokenProto{Identifier: []byte{1}, Password: []byte{2}})
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err = wrapped(ctx, "tcp", "test:1")
	require.ErrorIs(t, err, transfer.ErrUnknownEncryptionKey)
	require.EqualValues(t, 2, fetches.Load())
	require.EqualValues(t, 2, dials.Load())
}

func TestKMSRequestTimeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		<-request.Context().Done()
	}))
	defer server.Close()
	client := &Client{http: &http.Client{Timeout: 20 * time.Millisecond}}
	_, _, err := client.kmsRequest(server.URL, nil)
	require.Error(t, err)
}

func TestEncryptionKeyExpiryRefreshIsCoalesced(t *testing.T) {
	var calls atomic.Int32
	client := newProtocolClient(t, func(method string, raw []byte) proto.Message {
		if method != "getDataEncryptionKey" {
			t.Errorf("unexpected method: %s", method)
		}
		calls.Add(1)
		return &hdfs.GetDataEncryptionKeyResponseProto{DataEncryptionKey: &hdfs.DataEncryptionKeyProto{
			KeyId: proto.Uint32(2), BlockPoolId: proto.String("bp"), Nonce: []byte{1}, EncryptionKey: []byte("1234567890123456"),
			ExpiryDate: proto.Uint64(uint64(time.Now().Add(time.Hour).UnixMilli())),
		}}
	})
	client.encryptionKey = &hdfs.DataEncryptionKeyProto{KeyId: proto.Uint32(1), ExpiryDate: proto.Uint64(1)}
	var workers sync.WaitGroup
	for i := 0; i < 16; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			key, err := client.fetchDataEncryptionKeyContext(context.Background())
			if err != nil || key.GetKeyId() != 2 {
				t.Errorf("key=%v error=%v", key, err)
			}
		}()
	}
	workers.Wait()
	require.EqualValues(t, 1, calls.Load())
}
