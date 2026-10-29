package transfer

import (
	"crypto/aes"
	"crypto/cipher"
	"fmt"
	"net"
	"sync"
	"time"
)

type aesConn struct {
	conn     net.Conn
	readMu   sync.Mutex
	writeMu  sync.Mutex
	writeErr error

	encStream cipher.StreamWriter
	decStream cipher.StreamReader
}

func newAesConn(conn net.Conn, inKey, outKey, inIv, outIv []byte) (net.Conn, error) {
	if len(inIv) != aes.BlockSize || len(outIv) != aes.BlockSize {
		return nil, fmt.Errorf("invalid AES IV length")
	}
	c := &aesConn{conn: conn}

	encBlock, err := aes.NewCipher(inKey)
	if err != nil {
		return nil, err
	}

	decBlock, err := aes.NewCipher(outKey)
	if err != nil {
		return nil, err
	}

	c.encStream = cipher.StreamWriter{S: cipher.NewCTR(encBlock, inIv), W: conn}
	c.decStream = cipher.StreamReader{S: cipher.NewCTR(decBlock, outIv), R: conn}
	return c, nil
}

func (d *aesConn) Close() error {
	return d.conn.Close()
}

func (d *aesConn) LocalAddr() net.Addr {
	return d.conn.LocalAddr()
}

func (d *aesConn) RemoteAddr() net.Addr {
	return d.conn.RemoteAddr()
}

func (d *aesConn) SetDeadline(t time.Time) error {
	return d.conn.SetDeadline(t)
}

func (d *aesConn) SetReadDeadline(t time.Time) error {
	return d.conn.SetReadDeadline(t)
}

func (d *aesConn) SetWriteDeadline(t time.Time) error {
	return d.conn.SetWriteDeadline(t)
}

func (d *aesConn) Write(b []byte) (n int, err error) {
	d.writeMu.Lock()
	defer d.writeMu.Unlock()
	if d.writeErr != nil {
		return 0, d.writeErr
	}
	n, err = d.encStream.Write(b)
	if err != nil {
		d.writeErr = err
		d.conn.Close()
	}
	return n, err
}

func (d *aesConn) Read(b []byte) (n int, err error) {
	d.readMu.Lock()
	defer d.readMu.Unlock()
	return d.decStream.Read(b)
}
