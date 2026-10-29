package transfer

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"testing"

	hdfs "github.com/colinmarc/hdfs/v2/internal/protocol/hadoop_hdfs"
	"github.com/stretchr/testify/require"
)

func TestReadPrefixedMessagePreservesNextShortMessage(t *testing.T) {
	packet, err := makePrefixedMessage(&hdfs.BlockOpResponseProto{Status: hdfs.Status_SUCCESS.Enum()})
	require.NoError(t, err)
	require.Len(t, packet, 3)
	reader := bytes.NewReader(append(append([]byte{}, packet...), packet...))
	for i := 0; i < 2; i++ {
		response, err := readBlockOpResponse(reader)
		require.NoError(t, err)
		require.Equal(t, hdfs.Status_SUCCESS, response.GetStatus())
	}
	require.Zero(t, reader.Len())
}

func TestReadPrefixedMessageRejectsOversizeBeforePayload(t *testing.T) {
	var prefix [binary.MaxVarintLen32]byte
	n := binary.PutUvarint(prefix[:], maxDataTransferMessageLength+1)
	err := readPrefixedMessage(bytes.NewReader(prefix[:n]), &hdfs.BlockOpResponseProto{})
	require.ErrorIs(t, err, errInvalidResponse)
}

func TestBlockReadStreamRejectsInvalidPacketMetadata(t *testing.T) {
	packet := readPacket(t, []byte("abcd"), 4, false)
	for _, chunkSize := range []int{0, -1} {
		_, err := newBlockReadStream(bytes.NewReader(packet), chunkSize, crc32.IEEETable, 4).Read(make([]byte, 4))
		require.ErrorIs(t, err, errInvalidResponse)
	}
	badLength := append([]byte{}, packet...)
	binary.BigEndian.PutUint32(badLength, 4)
	_, err := newBlockReadStream(bytes.NewReader(badLength), 4, crc32.IEEETable, 4).Read(make([]byte, 4))
	require.ErrorIs(t, err, errInvalidResponse)
	badHeader := append([]byte{}, packet...)
	badHeader[6] = 0xff
	_, err = newBlockReadStream(bytes.NewReader(badHeader), 4, crc32.IEEETable, 4).Read(make([]byte, 4))
	require.Error(t, err)
}

func FuzzReadPrefixedMessage(f *testing.F) {
	f.Add([]byte{2, 8, 0})
	f.Add([]byte{255, 255, 255, 255, 255})
	f.Fuzz(func(t *testing.T, wire []byte) {
		_ = readPrefixedMessage(bytes.NewReader(wire), &hdfs.BlockOpResponseProto{})
	})
}

func FuzzBlockReadStream(f *testing.F) {
	f.Add([]byte{0, 0, 0, 4, 0, 0}, uint32(512))
	f.Fuzz(func(t *testing.T, wire []byte, chunkSize uint32) {
		stream := newBlockReadStream(bytes.NewReader(wire), int(chunkSize), crc32.IEEETable, 4)
		_, _ = stream.Read(make([]byte, 1024))
	})
}
