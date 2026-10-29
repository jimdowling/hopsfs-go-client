package hdfs

import (
	"context"
	hdfs "github.com/colinmarc/hdfs/v2/internal/protocol/hadoop_hdfs"
)

// ServerDefaults represents the filesystem configuration stored on the
// Namenode.
type ServerDefaults struct {
	BlockSize           int64
	BytesPerChecksum    int
	WritePacketSize     int
	Replication         int
	FileBufferSize      int
	EncryptDataTransfer bool
	TrashInterval       int64
	KeyProviderURI      string
	PolicyId            int
}

// ServerDefaults fetches the stored defaults from the Namenode and returns
// them and any error encountered.
func (c *Client) ServerDefaults() (ServerDefaults, error) {
	resp, err := c.fetchDefaults()
	if err != nil {
		return ServerDefaults{}, err
	}

	return ServerDefaults{
		BlockSize:           int64(resp.GetBlockSize()),
		BytesPerChecksum:    int(resp.GetBytesPerChecksum()),
		WritePacketSize:     int(resp.GetWritePacketSize()),
		Replication:         int(resp.GetReplication()),
		FileBufferSize:      int(resp.GetFileBufferSize()),
		EncryptDataTransfer: resp.GetEncryptDataTransfer(),
		TrashInterval:       int64(resp.GetTrashInterval()),
		KeyProviderURI:      resp.GetKeyProviderUri(),
		PolicyId:            int(resp.GetPolicyId()),
	}, nil
}

type defaultsLoad struct {
	done     chan struct{}
	defaults *hdfs.FsServerDefaultsProto
	err      error
}

func (c *Client) fetchDefaults() (*hdfs.FsServerDefaultsProto, error) {
	return c.fetchDefaultsContext(context.Background())
}

func (c *Client) fetchDefaultsContext(ctx context.Context) (*hdfs.FsServerDefaultsProto, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.defaultsMu.Lock()
	if c.defaults != nil {
		defaults := c.defaults
		c.defaultsMu.Unlock()
		return defaults, nil
	}
	if loading := c.defaultsLoading; loading != nil {
		c.defaultsMu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-loading.done:
			return loading.defaults, loading.err
		}
	}
	loading := &defaultsLoad{done: make(chan struct{})}
	c.defaultsLoading = loading
	c.defaultsMu.Unlock()
	response := &hdfs.GetServerDefaultsResponseProto{}
	err := c.namenode.ExecuteContext(ctx, "getServerDefaults", &hdfs.GetServerDefaultsRequestProto{}, response)
	c.defaultsMu.Lock()
	loading.err = interpretException(err)
	if err == nil {
		loading.defaults = response.GetServerDefaults()
		c.defaults = loading.defaults
	}
	c.defaultsLoading = nil
	close(loading.done)
	c.defaultsMu.Unlock()
	return loading.defaults, loading.err
}
