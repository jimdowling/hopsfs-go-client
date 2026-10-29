package transfer

import (
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type saslBufferConn struct{ bytes.Buffer }

func (c *saslBufferConn) Close() error                     { return nil }
func (c *saslBufferConn) LocalAddr() net.Addr              { return &net.TCPAddr{} }
func (c *saslBufferConn) RemoteAddr() net.Addr             { return &net.TCPAddr{} }
func (c *saslBufferConn) SetDeadline(time.Time) error      { return nil }
func (c *saslBufferConn) SetReadDeadline(time.Time) error  { return nil }
func (c *saslBufferConn) SetWriteDeadline(time.Time) error { return nil }

func testProtectedConn(conn net.Conn, privacy bool) digestMD5Conn {
	key := []byte("1234567890123456")
	if privacy {
		return newDigestMD5PrivacyConn(conn, key, key, key, key)
	}
	return newDigestMD5IntegrityConn(conn, key, key)
}

func TestProtectedStreamPreservesBytesAcrossFrames(t *testing.T) {
	for _, privacy := range []bool{false, true} {
		for split := 1; split < 10; split++ {
			wire := &saslBufferConn{}
			writer := testProtectedConn(wire, privacy)
			n, err := writer.Write([]byte("abcdef"))
			require.NoError(t, err)
			require.Equal(t, 6, n)
			n, err = writer.Write([]byte("ghij"))
			require.NoError(t, err)
			require.Equal(t, 4, n)
			reader := testProtectedConn(wire, privacy)
			first, rest := make([]byte, split), make([]byte, 10-split)
			_, err = io.ReadFull(reader, first)
			require.NoError(t, err)
			_, err = io.ReadFull(reader, rest)
			require.NoError(t, err)
			require.Equal(t, "abcdefghij", string(append(first, rest...)))
		}
	}
}

func TestProtectedStreamRejectsInvalidLengths(t *testing.T) {
	for _, privacy := range []bool{false, true} {
		for _, length := range []uint32{0, 4, 15, 0xffffffff, maxDataTransferMessageLength + 1} {
			wire := &saslBufferConn{}
			require.NoError(t, binary.Write(wire, binary.BigEndian, length))
			_, err := testProtectedConn(wire, privacy).Read(make([]byte, 1))
			require.ErrorIs(t, err, errInvalidResponse)
		}
	}
}

func TestProtectedStreamReturnsBufferedDataBeforeNextFrameError(t *testing.T) {
	for _, privacy := range []bool{false, true} {
		wire := &saslBufferConn{}
		_, err := testProtectedConn(wire, privacy).Write([]byte("abcdef"))
		require.NoError(t, err)
		reader := testProtectedConn(wire, privacy)
		_, err = reader.Read(make([]byte, 2))
		require.NoError(t, err)
		b := make([]byte, 8)
		n, err := reader.Read(b)
		require.NoError(t, err)
		require.Equal(t, "cdef", string(b[:n]))
		_, err = reader.Read(b)
		require.ErrorIs(t, err, io.EOF)
	}
}

func TestProtectedStreamDoesNotExposePartialUnauthenticatedFrame(t *testing.T) {
	for _, privacy := range []bool{false, true} {
		wire := &saslBufferConn{}
		_, err := testProtectedConn(wire, privacy).Write([]byte("secret payload"))
		require.NoError(t, err)
		wire.Truncate(wire.Len() - 5)
		reader := testProtectedConn(wire, privacy)
		for attempt := 0; attempt < 2; attempt++ {
			n, err := reader.Read(make([]byte, 100))
			require.Zero(t, n)
			require.Error(t, err)
		}
	}
}

func TestMalformedSASLCompletionAndIVReturnErrors(t *testing.T) {
	_, err := newAesConn(nil, nil, nil, nil, nil)
	require.Error(t, err)
	require.Error(t, (&digestMD5Handshake{}).challengeStep2([]byte("rspauth")))
}

func TestProtectedConnectionsSupportConcurrentCalls(t *testing.T) {
	key := []byte("1234567890123456")
	factories := map[string]func(net.Conn) net.Conn{
		"integrity": func(conn net.Conn) net.Conn { return testProtectedConn(conn, false) },
		"privacy":   func(conn net.Conn) net.Conn { return testProtectedConn(conn, true) },
		"aes": func(conn net.Conn) net.Conn {
			wrapped, err := newAesConn(conn, key, key, key, key)
			require.NoError(t, err)
			return wrapped
		},
	}
	for name, factory := range factories {
		t.Run(name, func(t *testing.T) {
			left, right := net.Pipe()
			defer left.Close()
			defer right.Close()
			require.NoError(t, left.SetDeadline(time.Now().Add(5*time.Second)))
			require.NoError(t, right.SetDeadline(time.Now().Add(5*time.Second)))
			writer, reader := factory(left), factory(right)
			const calls, size = 16, 128
			results := make(chan []byte, calls)
			errors := make(chan error, calls*2)
			var workers sync.WaitGroup
			for i := 0; i < calls; i++ {
				workers.Add(2)
				go func(value byte) {
					defer workers.Done()
					n, err := writer.Write(bytes.Repeat([]byte{value}, size))
					if err == nil && n != size {
						err = io.ErrShortWrite
					}
					errors <- err
				}(byte(i))
				go func() {
					defer workers.Done()
					data := make([]byte, size)
					_, err := io.ReadFull(reader, data)
					errors <- err
					results <- data
				}()
			}
			workers.Wait()
			close(errors)
			close(results)
			for err := range errors {
				require.NoError(t, err)
			}
			counts := make([]int, 256)
			for data := range results {
				for _, value := range data {
					counts[int(value)]++
				}
			}
			for i := 0; i < calls; i++ {
				require.Equal(t, size, counts[i])
			}
		})
	}
}

func FuzzProtectedStreamDecode(f *testing.F) {
	f.Add([]byte{}, false)
	f.Add([]byte{1, 2, 3, 4}, true)
	f.Fuzz(func(t *testing.T, data []byte, privacy bool) {
		_, _ = testProtectedConn(&saslBufferConn{}, privacy).decode(data)
	})
}
