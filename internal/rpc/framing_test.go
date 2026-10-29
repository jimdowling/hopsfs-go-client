package rpc

import (
	"bytes"
	"testing"

	hadoop "github.com/colinmarc/hdfs/v2/internal/protocol/hadoop_common"
	hdfs "github.com/colinmarc/hdfs/v2/internal/protocol/hadoop_hdfs"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestReadRPCPacketRejectsInvalidInnerLength(t *testing.T) {
	for _, packet := range [][]byte{
		{0, 0, 0, 1, 1},
		{0, 0, 0, 1, 0x80},
		{0, 0, 0, 0},
	} {
		require.Error(t, readRPCPacket(bytes.NewReader(packet), 1024, &hadoop.RpcResponseHeaderProto{}))
	}
}

func TestReadRPCPacketRequiresSuccessPayloadButAllowsErrorOnly(t *testing.T) {
	for _, status := range []hadoop.RpcResponseHeaderProto_RpcStatusProto{
		hadoop.RpcResponseHeaderProto_SUCCESS, hadoop.RpcResponseHeaderProto_ERROR,
	} {
		header := &hadoop.RpcResponseHeaderProto{CallId: proto.Uint32(1), Status: status.Enum()}
		packet, err := makeRPCPacket(header)
		require.NoError(t, err)
		err = readRPCPacket(bytes.NewReader(packet), 1024, &hadoop.RpcResponseHeaderProto{}, &hdfs.RenewLeaseResponseProto{})
		if status == hadoop.RpcResponseHeaderProto_SUCCESS {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
		}
	}
}

func FuzzReadRPCPacket(f *testing.F) {
	f.Add([]byte{0, 0, 0, 1, 1})
	f.Add([]byte{0, 0, 0, 1, 0x80})
	f.Fuzz(func(t *testing.T, wire []byte) {
		_ = readRPCPacket(bytes.NewReader(wire), 1024, &hadoop.RpcResponseHeaderProto{}, &hdfs.RenewLeaseResponseProto{})
	})
}
