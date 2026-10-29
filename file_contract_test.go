package hdfs

import (
	hdfs "github.com/colinmarc/hdfs/v2/internal/protocol/hadoop_hdfs"
	"google.golang.org/protobuf/proto"
	"math"
	"testing"
)

func TestProtocolEmptyAddressesReturnError(t *testing.T) {
	defer func() {
		if p := recover(); p != nil {
			t.Errorf("invalid options panic: %v", p)
		}
	}()
	if _, err := NewClient(ClientOptions{User: "review"}); err == nil {
		t.Error("expected validation error")
	}
}
func TestProtocolReadAtPreservesOffset(t *testing.T) {
	f := &FileReader{offset: 2, info: newFileInfo(&hdfs.HdfsFileStatusProto{Length: proto.Uint64(8)}, "test"), blocks: []*hdfs.LocatedBlockProto{{Offset: proto.Uint64(0), B: &hdfs.ExtendedBlockProto{NumBytes: proto.Uint64(8)}, Data: []byte("abcdefgh")}}}
	// A zero-byte ReadAt exercises cursor semantics without needing a client.
	if _, err := f.ReadAt(nil, 5); err != nil {
		t.Fatal(err)
	}
	if f.offset != 2 {
		t.Fatalf("ReadAt changed sequential offset from 2 to %d", f.offset)
	}
}
func TestProtocolAvailableClampsAtInt32(t *testing.T) {
	f := &FileReader{info: newFileInfo(&hdfs.HdfsFileStatusProto{Length: proto.Uint64(3 << 30)}, "test")}
	if got := f.Available(); got != math.MaxInt32 {
		t.Fatalf("Available returned %d for 3 GiB file", got)
	}
}
func TestProtocolSmallWriteUpdatesPosition(t *testing.T) {
	f := &FileWriter{storeInDB: true}
	n, err := f.Write([]byte("hello"))
	if err != nil || n != 5 {
		t.Fatal(n, err)
	}
	if f.GetPos() != 5 {
		t.Fatalf("GetPos=%d after 5-byte successful write", f.GetPos())
	}
}
