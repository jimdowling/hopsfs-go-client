package transfer

import (
	"bytes"
	hmacpkg "crypto/hmac"
	"crypto/md5"
	"crypto/rc4"
	"encoding/binary"
	"errors"
	"hash"
	"io"
	"net"
	"sync"
	"time"
)

type digestMD5PrivacyConn struct {
	readMu   sync.Mutex
	writeMu  sync.Mutex
	readErr  error
	writeErr error
	conn     net.Conn

	sendSeqNum int
	readSeqNum int

	decodeMAC hash.Hash
	encodeMAC hash.Hash

	decryptor *rc4.Cipher
	encryptor *rc4.Cipher

	readBuf  bytes.Buffer
	writeBuf bytes.Buffer
}

// digestMD5PrivacyConn returns a net.Conn wrapper that peforms md5-digest
// encryption on data passing over it.
func newDigestMD5PrivacyConn(conn net.Conn, kic, kis, kcc, kcs []byte) digestMD5Conn {
	encryptor, _ := rc4.NewCipher(kcc)
	decryptor, _ := rc4.NewCipher(kcs)

	return &digestMD5PrivacyConn{
		conn:      conn,
		encryptor: encryptor,
		decryptor: decryptor,
		decodeMAC: hmacpkg.New(md5.New, kis),
		encodeMAC: hmacpkg.New(md5.New, kic),
	}
}

func (d *digestMD5PrivacyConn) Close() error {
	return d.conn.Close()
}

func (d *digestMD5PrivacyConn) LocalAddr() net.Addr {
	return d.conn.LocalAddr()
}

func (d *digestMD5PrivacyConn) RemoteAddr() net.Addr {
	return d.conn.RemoteAddr()
}

func (d *digestMD5PrivacyConn) SetDeadline(t time.Time) error {
	return d.conn.SetDeadline(t)
}

func (d *digestMD5PrivacyConn) SetReadDeadline(t time.Time) error {
	return d.conn.SetReadDeadline(t)
}

func (d *digestMD5PrivacyConn) SetWriteDeadline(t time.Time) error {
	return d.conn.SetWriteDeadline(t)
}

func (d *digestMD5PrivacyConn) Read(b []byte) (int, error) {
	d.readMu.Lock()
	defer d.readMu.Unlock()
	if d.readErr != nil {
		return 0, d.readErr
	}
	if len(b) == 0 {
		return 0, nil
	}
	if d.readBuf.Len() > 0 {
		return d.readBuf.Read(b)
	}
	for {
		if err := readSASLFrame(d.conn, &d.readBuf); err != nil {
			d.readBuf.Reset()
			d.readErr = err
			d.conn.Close()
			return 0, err
		}
		decoded, err := d.decode(d.readBuf.Bytes())
		if err != nil {
			d.readBuf.Reset()
			d.readErr = err
			d.conn.Close()
			return 0, err
		}
		d.readBuf.Truncate(len(decoded))
		if len(decoded) > 0 {
			return d.readBuf.Read(b)
		}
	}
}

func (d *digestMD5PrivacyConn) decode(input []byte) (out []byte, err error) {
	inputLen := len(input)
	if inputLen < saslIntegrityTrailerLength {
		return nil, errors.New("invalid response from datanode: bad response length")
	}

	seqNumStart := inputLen - macSeqNumLen
	msgTypeStart := seqNumStart - macMsgTypeLen

	encryptedLen := inputLen - macMsgTypeLen - macSeqNumLen
	d.decryptor.XORKeyStream(input[:encryptedLen], input[:encryptedLen])

	origHash := input[encryptedLen-macHMACLen : encryptedLen]
	encryptedLen -= macHMACLen

	seqBuf := lenEncodeBytes(d.readSeqNum)
	hmac := msgHMAC(d.decodeMAC, seqBuf, input[:encryptedLen])

	msgType := input[msgTypeStart : msgTypeStart+macMsgTypeLen]
	seqNum := input[seqNumStart : seqNumStart+macSeqNumLen]

	if !hmacpkg.Equal(hmac, origHash) || !bytes.Equal(macMsgType[:], msgType) || !bytes.Equal(seqNum, seqBuf[:]) {
		return nil, errors.New("invalid response from datanode: HMAC check failed")
	}

	d.readSeqNum++
	return input[:encryptedLen], nil
}

func (d *digestMD5PrivacyConn) Write(b []byte) (int, error) {
	d.writeMu.Lock()
	defer d.writeMu.Unlock()
	if d.writeErr != nil {
		return 0, d.writeErr
	}
	inputLen := len(b)
	if inputLen > maxDataTransferMessageLength-saslIntegrityTrailerLength {
		return 0, errInvalidResponse
	}
	seqBuf := lenEncodeBytes(d.sendSeqNum)

	encryptedLen := inputLen + macHMACLen
	outputLen := macDataLen + encryptedLen + macMsgTypeLen + macSeqNumLen
	d.writeBuf.Reset()
	d.writeBuf.Grow(outputLen)

	finalLength := encryptedLen + macMsgTypeLen + macSeqNumLen
	binary.Write(&d.writeBuf, binary.BigEndian, int32(finalLength))
	d.writeBuf.Write(b)

	hmac := msgHMAC(d.encodeMAC, seqBuf, b)
	d.writeBuf.Write(hmac)

	toEncrypt := d.writeBuf.Bytes()[macDataLen:]
	d.encryptor.XORKeyStream(toEncrypt, toEncrypt)
	d.writeBuf.Write(macMsgType[:])
	binary.Write(&d.writeBuf, binary.BigEndian, int32(d.sendSeqNum))

	d.sendSeqNum++
	n, err := d.writeBuf.WriteTo(d.conn)
	if err != nil {
		d.writeErr = err
		d.conn.Close()
		return 0, err
	}
	if int(n) != outputLen {
		d.writeErr = io.ErrShortWrite
		d.conn.Close()
		return 0, io.ErrShortWrite
	}
	return inputLen, nil
}
