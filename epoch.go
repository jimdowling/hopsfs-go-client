package hdfs

import (
	"time"

	hdfs "github.com/colinmarc/hdfs/v2/internal/protocol/hadoop_hdfs"
)

// Get epoch from the namenode
func (c *Client) getNNEpochMS() (int64, error) {

	req := &hdfs.GetEpochMSRequestProto{}
	resp := &hdfs.GetEpochMSResponseProto{}

	err := c.leaderNamenode.Execute("getNNEpochMS", req, resp)
	if err != nil {
		return 0, interpretException(err)
	}

	return resp.GetEpoch(), nil
}

func (c *Client) setEpoch() error {
	start := time.Now()
	epoch, err := c.getNNEpochMS()
	endTime := time.Now()
	diff := time.Since(start).Milliseconds() / 2

	if err != nil {
		return err
	}
	c.namenode.SetEpoch(endTime, epoch+diff)
	c.leaderNamenode.SetEpoch(endTime, epoch+diff)

	return nil
}
