package hdfs

import (
	"crypto/aes"
	"crypto/cipher"
	"io"
	"os"
	"sync"
	"testing"

	hdfs "github.com/colinmarc/hdfs/v2/internal/protocol/hadoop_hdfs"
	"github.com/colinmarc/hdfs/v2/internal/rpc"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func protocolStatus(name string, kind hdfs.HdfsFileStatusProto_FileType, size uint64) *hdfs.HdfsFileStatusProto {
	return &hdfs.HdfsFileStatusProto{
		FileType: kind.Enum(), Path: []byte(name), Length: proto.Uint64(size),
		Permission: &hdfs.FsPermissionProto{Perm: proto.Uint32(0644)},
		Owner:      proto.String("user"), Group: proto.String("group"),
		ModificationTime: proto.Uint64(0), AccessTime: proto.Uint64(0),
	}
}

func TestDirectoryIterationRetainsUnusedPage(t *testing.T) {
	calls := 0
	client := newProtocolClient(t, func(method string, raw []byte) proto.Message {
		require.Equal(t, "getListing", method)
		calls++
		return &hdfs.GetListingResponseProto{DirList: &hdfs.DirectoryListingProto{
			PartialListing: []*hdfs.HdfsFileStatusProto{
				protocolStatus("a", hdfs.HdfsFileStatusProto_IS_FILE, 0),
				protocolStatus("b", hdfs.HdfsFileStatusProto_IS_FILE, 0),
				protocolStatus("c", hdfs.HdfsFileStatusProto_IS_FILE, 0),
			}, RemainingEntries: proto.Uint32(0),
		}}
	})
	file := &FileReader{client: client, name: "/dir", info: newFileInfo(protocolStatus("dir", hdfs.HdfsFileStatusProto_IS_DIR, 0), "/dir")}
	first, err := file.Readdirnames(1)
	require.NoError(t, err)
	require.Equal(t, []string{"a"}, first)
	rest, err := file.Readdirnames(0)
	require.NoError(t, err)
	require.Equal(t, []string{"b", "c"}, rest)
	_, err = file.Readdirnames(1)
	require.ErrorIs(t, err, io.EOF)
	require.Equal(t, 1, calls)
}

func TestDirectoryIterationReturnsEntriesBeforeError(t *testing.T) {
	calls := 0
	client := newProtocolClient(t, func(string, []byte) proto.Message {
		calls++
		if calls > 1 {
			return &hdfs.GetListingResponseProto{}
		}
		return &hdfs.GetListingResponseProto{DirList: &hdfs.DirectoryListingProto{
			PartialListing:   []*hdfs.HdfsFileStatusProto{protocolStatus("a", hdfs.HdfsFileStatusProto_IS_FILE, 0)},
			RemainingEntries: proto.Uint32(1),
		}}
	})
	file := &FileReader{client: client, name: "/dir", info: newFileInfo(protocolStatus("dir", hdfs.HdfsFileStatusProto_IS_DIR, 0), "/dir")}
	names, err := file.Readdirnames(0)
	require.ErrorIs(t, err, os.ErrNotExist)
	require.Equal(t, []string{"a"}, names)
}

func TestParallelReadAtPreservesSequentialReader(t *testing.T) {
	for _, encrypted := range []bool{false, true} {
		t.Run(map[bool]string{false: "plain", true: "encrypted"}[encrypted], func(t *testing.T) {
			checkParallelReadAt(t, encrypted)
		})
	}
}

func checkParallelReadAt(t *testing.T, encrypted bool) {
	t.Helper()
	file := &FileReader{
		client: &Client{namenode: &rpc.NamenodeConnection{ClientName: "test"}},
		info:   newFileInfo(protocolStatus("file", hdfs.HdfsFileStatusProto_IS_FILE, 8), "file"), offset: 2,
		blocks: []*hdfs.LocatedBlockProto{{Offset: proto.Uint64(0), B: &hdfs.ExtendedBlockProto{NumBytes: proto.Uint64(8)}, Data: []byte("abcdefgh")}},
	}
	if encrypted {
		key, iv := []byte("1234567890123456"), make([]byte, aes.BlockSize)
		block, err := aes.NewCipher(key)
		require.NoError(t, err)
		cipher.NewCTR(block, iv).XORKeyStream(file.blocks[0].Data, file.blocks[0].Data)
		file.enc = &transparentEncryptionInfo{key: key, iv: iv}
	}
	var workers sync.WaitGroup
	for offset := 0; offset < 8; offset++ {
		workers.Add(1)
		go func(offset int) {
			defer workers.Done()
			var out [1]byte
			n, err := file.ReadAt(out[:], int64(offset))
			if err != nil || n != 1 || out[0] != "abcdefgh"[offset] {
				t.Errorf("offset=%d n=%d bytes=%q error=%v", offset, n, out[:], err)
			}
		}(offset)
	}
	workers.Wait()
	var out [1]byte
	_, err := file.Read(out[:])
	require.NoError(t, err)
	require.Equal(t, byte('c'), out[0])
	var pastEnd [4]byte
	n, err := file.ReadAt(pastEnd[:], 6)
	require.Equal(t, 2, n)
	require.ErrorIs(t, err, io.EOF)
	require.Equal(t, "gh", string(pastEnd[:n]))
	require.Equal(t, uint64(3), file.GetPos())
}
