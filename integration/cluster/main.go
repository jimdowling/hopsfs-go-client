// Command cluster validates the client against a real HopsFS deployment.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	hdfs "github.com/colinmarc/hdfs/v2"
)

type outcome struct {
	Name      string `json:"name"`
	Status    string `json:"status"`
	ElapsedMS int64  `json:"elapsed_ms"`
	Error     string `json:"error,omitempty"`
	Details   any    `json:"details,omitempty"`
}

var encoder = json.NewEncoder(os.Stdout)

type trackedConn struct {
	net.Conn
	onClose func()
	once    sync.Once
	err     error
}

func (conn *trackedConn) Close() error {
	conn.once.Do(func() { conn.err = conn.Conn.Close(); conn.onClose() })
	return conn.err
}

func countConnections(active *atomic.Int64) func(context.Context, string, string) (net.Conn, error) {
	return func(ctx context.Context, network, address string) (net.Conn, error) {
		conn, err := (&net.Dialer{}).DialContext(ctx, network, address)
		if err != nil {
			return nil, err
		}
		active.Add(1)
		return &trackedConn{Conn: conn, onClose: func() { active.Add(-1) }}, nil
	}
}

func main() {
	address := flag.String("address", "namenode.hopsworks:8020", "NameNode address")
	identity := flag.String("server-name", "namenode.hopsworks", "TLS certificate identity")
	certs := flag.String("certs", "/srv/hops/super_crypto/hdfs", "read-only HDFS certificate directory")
	root := flag.String("root", "", "unique test directory below /tmp/hopsfs-client-426-")
	policy := flag.String("policy", "HOT", "storage policy already assigned to root")
	cleanup := flag.Bool("cleanup", false, "remove only the supplied test directory")
	flag.Parse()
	if !strings.HasPrefix(*root, "/tmp/hopsfs-client-426-") || path.Clean(*root) != *root {
		fmt.Fprintln(os.Stderr, "root must be a clean, unique /tmp/hopsfs-client-426- path")
		os.Exit(2)
	}
	options := hdfs.ClientOptions{Addresses: []string{*address}, User: "hdfs", TLS: true,
		TLSServerName: *identity, RootCABundle: path.Join(*certs, "hops_ca_bundle.pem"),
		ClientCertificate: path.Join(*certs, "hdfs_certificate_bundle.pem"), ClientKey: path.Join(*certs, "hdfs_priv.pem"),
		NamenodeDialTimeout: 5 * time.Second, NamenodeRequestTimeout: 15 * time.Second}
	var activeNameNodes, activeDataNodes atomic.Int64
	options.NamenodeDialFunc = countConnections(&activeNameNodes)
	options.DatanodeDialFunc = countConnections(&activeDataNodes)
	c, err := hdfs.NewClient(options)
	if err != nil {
		encoder.Encode(outcome{Name: "connect", Status: "FAIL", Error: err.Error()})
		os.Exit(1)
	}
	defer c.Close()
	if *cleanup {
		err = c.RemoveAll(*root)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		_, err = c.Stat(*root)
		if !os.IsNotExist(err) {
			fmt.Fprintln(os.Stderr, "cleanup did not remove test root", err)
			os.Exit(1)
		}
		encoder.Encode(outcome{Name: "cleanup", Status: "PASS"})
		return
	}
	failed := 0
	run := func(name string, check func() (any, error)) {
		start := time.Now()
		details, err := check()
		record := outcome{Name: *policy + "/" + name, Status: "PASS", ElapsedMS: time.Since(start).Milliseconds(), Details: details}
		if err != nil {
			record.Status = "FAIL"
			record.Error = err.Error()
			failed++
		}
		encoder.Encode(record)
	}
	if err := c.MkdirAll(*root, 0700); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	file := func(name string) string { return path.Join(*root, name) }
	run("tls-rejects-wrong-identity", func() (any, error) {
		wrong := options
		wrong.TLSServerName = "wrong.invalid"
		other, err := hdfs.NewClient(wrong)
		if err == nil {
			other.Close()
			return nil, errors.New("accepted an incorrect TLS server identity")
		}
		if !strings.Contains(err.Error(), "certificate") {
			return nil, fmt.Errorf("failed for another reason: %w", err)
		}
		return nil, nil
	})
	run("concurrent-defaults", func() (any, error) {
		var workers sync.WaitGroup
		errorsCh := make(chan error, 32)
		for i := 0; i < 32; i++ {
			workers.Add(1)
			go func() {
				defer workers.Done()
				d, err := c.ServerDefaults()
				if err == nil && (d.BlockSize <= 0 || d.Replication <= 0) {
					err = errors.New("invalid defaults")
				}
				errorsCh <- err
			}()
		}
		workers.Wait()
		close(errorsCh)
		for err := range errorsCh {
			if err != nil {
				return nil, err
			}
		}
		d, err := c.ServerDefaults()
		return d, err
	})
	run("bootstrap-address-failover", func() (any, error) {
		fallback := options
		fallback.Addresses = []string{"127.0.0.1:1", *address}
		other, err := hdfs.NewClient(fallback)
		if err != nil {
			return nil, err
		}
		defer other.Close()
		_, err = other.Stat(*root)
		return nil, err
	})
	payload := make([]byte, 2*(1<<20)+37)
	rand.New(rand.NewSource(426)).Read(payload)
	run("multi-block-roundtrip", func() (any, error) {
		if err := writeFile(c, file("large"), payload, 34001); err != nil {
			return nil, err
		}
		got, err := c.ReadFile(file("large"))
		if err != nil {
			return nil, err
		}
		if !bytes.Equal(got, payload) {
			return nil, errors.New("multi-block contents differ")
		}
		digest := sha256.Sum256(got)
		return map[string]any{"bytes": len(got), "sha256": hex.EncodeToString(digest[:])}, nil
	})
	run("tiny-tail-reads", func() (any, error) {
		r, err := c.Open(file("large"))
		if err != nil {
			return nil, err
		}
		defer r.Close()
		start := int64(len(payload) - 1031)
		if _, err = r.Seek(start, io.SeekStart); err != nil {
			return nil, err
		}
		var got []byte
		var one [1]byte
		for {
			n, err := r.Read(one[:])
			got = append(got, one[:n]...)
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, err
			}
		}
		if !bytes.Equal(got, payload[start:]) {
			return nil, errors.New("tail bytes lost")
		}
		return len(got), nil
	})
	run("parallel-reader-at-and-eof", func() (any, error) {
		r, err := c.Open(file("large"))
		if err != nil {
			return nil, err
		}
		defer r.Close()
		if _, err = r.Seek(17, io.SeekStart); err != nil {
			return nil, err
		}
		var workers sync.WaitGroup
		errorsCh := make(chan error, 32)
		for i := 0; i < 32; i++ {
			workers.Add(1)
			go func(i int) {
				defer workers.Done()
				offset := (i * 65537) % (len(payload) - 2048)
				b := make([]byte, 2048)
				n, err := r.ReadAt(b, int64(offset))
				if err == nil && (n != len(b) || !bytes.Equal(b, payload[offset:offset+len(b)])) {
					err = errors.New("parallel ReadAt data mismatch")
				}
				errorsCh <- err
			}(i)
		}
		workers.Wait()
		close(errorsCh)
		for err := range errorsCh {
			if err != nil {
				return nil, err
			}
		}
		var next [3]byte
		if _, err = io.ReadFull(r, next[:]); err != nil {
			return nil, err
		}
		if !bytes.Equal(next[:], payload[17:20]) {
			return nil, errors.New("ReadAt moved sequential cursor")
		}
		b := make([]byte, 8)
		n, err := r.ReadAt(b, int64(len(payload)-2))
		if n != 2 || err != io.EOF || !bytes.Equal(b[:n], payload[len(payload)-2:]) {
			return nil, fmt.Errorf("past EOF: %d %v", n, err)
		}
		return nil, nil
	})
	run("append-across-block-boundary", func() (any, error) {
		initial := payload[:(1<<20)-3]
		extra := payload[(1<<20)-3 : (1<<20)+65565]
		if err := writeFile(c, file("append-boundary"), initial, 65536); err != nil {
			return nil, err
		}
		w, err := c.Append(file("append-boundary"))
		if err != nil {
			return nil, err
		}
		defer abort(w)
		w.SetDeadline(time.Now().Add(time.Minute))
		if n, err := w.Write(extra); err != nil || n != len(extra) {
			return nil, fmt.Errorf("append write: %d %v", n, err)
		}
		if err := w.Close(); err != nil {
			return nil, err
		}
		want := append(append([]byte{}, initial...), extra...)
		got, err := c.ReadFile(file("append-boundary"))
		if err != nil {
			return nil, err
		}
		if !bytes.Equal(got, want) {
			return nil, errors.New("append contents differ")
		}
		return len(got), nil
	})
	run("repeated-append-and-empty-append", func() (any, error) {
		name := file("repeated-append")
		want := []byte("start")
		if err := writeFile(c, name, want, 7); err != nil {
			return nil, err
		}
		for i := 0; i < 8; i++ {
			w, err := c.Append(name)
			if err != nil {
				return nil, err
			}
			w.SetDeadline(time.Now().Add(time.Minute))
			part := bytes.Repeat([]byte{byte('a' + i)}, 513+i)
			_, err = w.Write(part)
			if err != nil {
				abort(w)
				return nil, err
			}
			if err = w.Close(); err != nil {
				abort(w)
				return nil, err
			}
			want = append(want, part...)
		}
		w, err := c.Append(name)
		if err != nil {
			return nil, err
		}
		if err = w.Close(); err != nil {
			abort(w)
			return nil, err
		}
		got, err := c.ReadFile(name)
		if err != nil {
			return nil, err
		}
		if !bytes.Equal(got, want) {
			return nil, errors.New("repeated append contents differ")
		}
		return len(got), nil
	})
	run("small-file-position-and-growth", func() (any, error) {
		w, err := c.CreateFile(file("promoted"), 1, 1<<20, 0600, true, false)
		if err != nil {
			return nil, err
		}
		defer abort(w)
		w.SetDeadline(time.Now().Add(time.Minute))
		prefix := []byte("small")
		if _, err = w.Write(prefix); err != nil {
			return nil, err
		}
		if w.GetPos() != uint64(len(prefix)) {
			return nil, fmt.Errorf("small-file position %d", w.GetPos())
		}
		extra := payload[:131101]
		if n, err := w.Write(extra); err != nil || n != len(extra) {
			return nil, fmt.Errorf("promotion: %d %v", n, err)
		}
		if w.GetPos() != uint64(len(prefix)+len(extra)) {
			return nil, errors.New("promotion position mismatch")
		}
		if err = w.Close(); err != nil {
			return nil, err
		}
		got, err := c.ReadFile(file("promoted"))
		if err != nil {
			return nil, err
		}
		if !bytes.Equal(got, append(prefix, extra...)) {
			return nil, errors.New("promotion contents differ")
		}
		return len(got), nil
	})
	run("directory-cursor", func() (any, error) {
		dir := file("directory")
		if err := c.MkdirAll(dir, 0700); err != nil {
			return nil, err
		}
		for i := 0; i < 64; i++ {
			if err := c.CreateEmptyFile(path.Join(dir, fmt.Sprintf("%03d", i))); err != nil {
				return nil, err
			}
		}
		r, err := c.Open(dir)
		if err != nil {
			return nil, err
		}
		defer r.Close()
		first, err := r.Readdirnames(1)
		if err != nil || len(first) != 1 {
			return nil, fmt.Errorf("first page: %v %v", first, err)
		}
		rest, err := r.Readdirnames(0)
		if err != nil {
			return nil, err
		}
		all := append(first, rest...)
		if len(all) != 64 {
			return nil, fmt.Errorf("got %d directory entries", len(all))
		}
		for i, name := range all {
			if name != fmt.Sprintf("%03d", i) {
				return nil, fmt.Errorf("entry %d: %s", i, name)
			}
		}
		_, err = r.Readdirnames(1)
		if err != io.EOF {
			return nil, fmt.Errorf("directory EOF: %v", err)
		}
		return len(all), nil
	})
	run("metadata-and-missing-path-errors", func() (any, error) {
		from, to := file("before"), file("after")
		if err := c.CreateEmptyFile(from); err != nil {
			return nil, err
		}
		if err := c.Rename(from, to); err != nil {
			return nil, err
		}
		if _, err := c.Stat(from); !os.IsNotExist(err) {
			return nil, fmt.Errorf("old source: %v", err)
		}
		if err := c.Remove(to); err != nil {
			return nil, err
		}
		if err := c.Remove(to); !os.IsNotExist(err) {
			return nil, fmt.Errorf("missing remove: %v", err)
		}
		if err := c.RemoveAll(to); err != nil {
			return nil, err
		}
		return nil, nil
	})
	run("expired-read-deadline", func() (any, error) {
		r, err := c.Open(file("large"))
		if err != nil {
			return nil, err
		}
		defer r.Close()
		r.SetDeadline(time.Now().Add(-time.Second))
		_, err = r.Read(make([]byte, 1024))
		if err == nil {
			return nil, errors.New("expired read deadline succeeded")
		}
		_, err = c.Stat(file("large"))
		return nil, err
	})
	run("terminal-write-deadline-and-abort", func() (any, error) {
		w, err := c.CreateFile(file("aborted"), 1, 1<<20, 0600, true, false)
		if err != nil {
			return nil, err
		}
		defer abort(w)
		w.SetDeadline(time.Now().Add(-time.Second))
		_, first := w.Write(payload[:65537])
		if first == nil {
			return nil, errors.New("expired write deadline succeeded")
		}
		if err = w.Close(); err == nil {
			return nil, errors.New("Close lost terminal error")
		}
		if _, err = w.Write([]byte("later")); err == nil {
			return nil, errors.New("failed writer accepted more data")
		}
		return nil, nil
	})
	run("checksum-repeated", func() (any, error) {
		var want []byte
		for i := 0; i < 10; i++ {
			r, err := c.Open(file("large"))
			if err != nil {
				return nil, err
			}
			got, err := r.Checksum()
			r.Close()
			if err != nil {
				return nil, err
			}
			if i == 0 {
				want = got
			} else if !bytes.Equal(want, got) {
				return nil, errors.New("checksum changed between calls")
			}
		}
		return hex.EncodeToString(want), nil
	})
	run("close-is-idempotent-and-final", func() (any, error) {
		other, err := hdfs.NewClient(options)
		if err != nil {
			return nil, err
		}
		if err = other.Close(); err != nil {
			return nil, err
		}
		if err = other.Close(); err != nil {
			return nil, err
		}
		if _, err = other.Stat(*root); err == nil {
			return nil, errors.New("client reconnected after Close")
		}
		return nil, nil
	})
	run("all-connections-released", func() (any, error) {
		if err := c.Close(); err != nil {
			return nil, err
		}
		counts := map[string]int64{"namenode_connections": activeNameNodes.Load(), "datanode_connections": activeDataNodes.Load()}
		if counts["namenode_connections"] != 0 || counts["datanode_connections"] != 0 {
			return counts, errors.New("connections retained after Close")
		}
		return counts, nil
	})
	encoder.Encode(map[string]any{"summary": true, "policy": *policy, "failed": failed, "root": *root})
	if failed > 0 {
		os.Exit(1)
	}
}

func abort(writer *hdfs.FileWriter) {
	if aborter, ok := any(writer).(interface{ Abort() error }); ok {
		aborter.Abort()
	} else {
		writer.Close()
	}
}

func writeFile(client *hdfs.Client, name string, data []byte, chunk int) error {
	w, err := client.CreateFile(name, 1, 1<<20, 0600, true, false)
	if err != nil {
		return err
	}
	defer abort(w)
	w.SetDeadline(time.Now().Add(time.Minute))
	for off := 0; off < len(data); {
		end := off + chunk
		if end > len(data) {
			end = len(data)
		}
		n, err := w.Write(data[off:end])
		off += n
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrNoProgress
		}
	}
	return w.Close()
}
