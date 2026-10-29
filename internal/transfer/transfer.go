// Package transfer implements wire transfer with the datanodes.
package transfer

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"

	hdfs "github.com/colinmarc/hdfs/v2/internal/protocol/hadoop_hdfs"
	"google.golang.org/protobuf/proto"
)

const (
	dataTransferVersion          = 0x1c
	writeBlockOp                 = 0x50
	readBlockOp                  = 0x51
	checksumBlockOp              = 0x55
	maxDataTransferMessageLength = 1 << 20
)

var errInvalidResponse = errors.New("invalid response from datanode")

func makePrefixedMessage(msg proto.Message) ([]byte, error) {
	msgBytes, err := proto.Marshal(msg)
	if err != nil {
		return nil, err
	}

	lengthBytes := make([]byte, binary.MaxVarintLen32)
	n := binary.PutUvarint(lengthBytes, uint64(len(msgBytes)))
	return append(lengthBytes[:n], msgBytes...), nil
}

func readPrefixedMessage(r io.Reader, msg proto.Message) error {
	var prefix [binary.MaxVarintLen32]byte
	for i := range prefix {
		if _, err := io.ReadFull(r, prefix[i:i+1]); err != nil {
			if err == io.EOF {
				return io.ErrUnexpectedEOF
			}
			return err
		}
		if prefix[i] >= 0x80 {
			continue
		}
		length, n := binary.Uvarint(prefix[:i+1])
		if n <= 0 || length > maxDataTransferMessageLength {
			return errInvalidResponse
		}
		payload := make([]byte, int(length))
		if _, err := io.ReadFull(r, payload); err != nil {
			if err == io.EOF {
				return io.ErrUnexpectedEOF
			}
			return err
		}
		return proto.Unmarshal(payload, msg)
	}
	return errInvalidResponse
}

// A op request to a datanode:
// +-----------------------------------------------------------+
// |  Data Transfer Protocol Version, int16                    |
// +-----------------------------------------------------------+
// |  Op code, 1 byte                                          |
// +-----------------------------------------------------------+
// |  varint length + OpReadBlockProto                         |
// +-----------------------------------------------------------+
func writeBlockOpRequest(w io.Writer, op uint8, msg proto.Message) error {
	header := []byte{0x00, dataTransferVersion, op}
	msgBytes, err := makePrefixedMessage(msg)
	if err != nil {
		return err
	}

	req := append(header, msgBytes...)
	_, err = w.Write(req)
	if err != nil {
		return err
	}

	return nil
}

// The initial response from a datanode, in the case of reads and writes:
// +-----------------------------------------------------------+
// |  varint length + BlockOpResponseProto                     |
// +-----------------------------------------------------------+
func readBlockOpResponse(r io.Reader) (*hdfs.BlockOpResponseProto, error) {
	resp := &hdfs.BlockOpResponseProto{}
	err := readPrefixedMessage(r, resp)

	return resp, err
}

const HOPSFS_CLOUD_DATANODE_HOSTNAME_OVERRIDE_ENV = "HOPSFS_CLOUD_DATANODE_HOSTNAME_OVERRIDE"
const HOPSFS_CLOUD_DATANODE_PORT_OVERRIDE_ENV = "HOPSFS_CLOUD_DATANODE_PORT_OVERRIDE"

func getDatanodeAddress(datanode *hdfs.DatanodeIDProto, useHostname bool) (string, error) {
	var host string
	if useHostname {
		host = datanode.GetHostName()
	} else {
		host = datanode.GetIpAddr()
	}
	port := datanode.GetXferPort()

	hostOverride := getEnv(HOPSFS_CLOUD_DATANODE_HOSTNAME_OVERRIDE_ENV)
	portOverride := getEnv(HOPSFS_CLOUD_DATANODE_PORT_OVERRIDE_ENV)

	if hostOverride != "" {
		host = hostOverride
	}

	if portOverride != "" {
		i, err := strconv.ParseUint(portOverride, 10, 16)
		if err != nil {
			return "", fmt.Errorf("invalid %s %q: %w", HOPSFS_CLOUD_DATANODE_PORT_OVERRIDE_ENV, portOverride, err)
		}
		port = uint32(i)
	}

	if port == 0 || port > 65535 {
		return "", fmt.Errorf("invalid datanode transfer port: %d", port)
	}
	return net.JoinHostPort(host, strconv.Itoa(int(port))), nil
}
