package hdfs

import (
	"errors"
	"os"

	hdfs "github.com/colinmarc/hdfs/v2/internal/protocol/hadoop_hdfs"
	"google.golang.org/protobuf/proto"
)

// Sets replication factor for a file
func (c *Client) SetReplication(name string, replication int16) (bool, error) {
	return setReplication(c, name, replication)
}

func setReplication(c *Client, name string, replication int16) (bool, error) {
	req := &hdfs.SetReplicationRequestProto{
		Src:         proto.String(name),
		Replication: proto.Uint32(uint32(replication)),
	}
	resp := &hdfs.SetReplicationResponseProto{}

	err := c.leaderNamenode.Execute("setReplication", req, resp)
	if err != nil {
		return false, &os.PathError{Op: "setReplication", Path: name,
			Err: interpretException(err)}
	} else if resp.Result == nil {
		return false, &os.PathError{Op: "setReplication", Path: name,
			Err: errors.New("unexpected empty response")}
	}

	if !resp.GetResult() {
		if _, err := c.getFileInfo(name); err != nil {
			return false, &os.PathError{Op: "setReplication", Path: name, Err: interpretException(err)}
		}
	}
	return *resp.Result, nil
}
