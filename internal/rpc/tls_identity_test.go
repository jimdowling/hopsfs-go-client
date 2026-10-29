package rpc

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestTLSChecksServerIdentity(t *testing.T) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "wrong.example"}, DNSNames: []string{"wrong.example"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, IsCA: true, BasicConstraintsValid: true}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	certFile := filepath.Join(dir, "cert.pem")
	keyFile := filepath.Join(dir, "key.pem")
	os.WriteFile(certFile, certPEM, 0600)
	os.WriteFile(keyFile, keyPEM, 0600)
	listener, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{Certificates: []tls.Certificate{cert}})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	done := make(chan struct{})
	go func() {
		defer close(done)
		c, e := listener.Accept()
		if e == nil {
			c.SetDeadline(time.Now().Add(time.Second))
			c.(*tls.Conn).Handshake()
			c.Close()
		}
	}()
	nn := &NamenodeConnection{RootCABundle: certFile, ClientCertificate: certFile, ClientKey: keyFile, dialTimeout: time.Second}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	c, err := nn.tlsDialFunction(ctx, "tcp", listener.Addr().String())
	if c != nil {
		c.Close()
	}
	<-done
	if err == nil {
		t.Error("accepted wrong.example certificate for 127.0.0.1 connection")
	}
	parsed, _ := x509.ParseCertificate(der)
	if err := parsed.VerifyHostname(net.ParseIP("127.0.0.1").String()); err == nil {
		t.Fatal("fixture unexpectedly valid for target")
	}
	overrideListener, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{Certificates: []tls.Certificate{cert}})
	if err != nil {
		t.Fatal(err)
	}
	defer overrideListener.Close()
	overrideDone := make(chan struct{})
	go func() {
		defer close(overrideDone)
		c, e := overrideListener.Accept()
		if e == nil {
			c.SetDeadline(time.Now().Add(time.Second))
			c.(*tls.Conn).Handshake()
			c.Close()
		}
	}()
	nn.TLSServerName = "wrong.example"
	c, err = nn.tlsDialFunction(ctx, "tcp", overrideListener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	c.Close()
	<-overrideDone

}
