package rpc

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	hadoop "github.com/colinmarc/hdfs/v2/internal/protocol/hadoop_common"
	hdfs "github.com/colinmarc/hdfs/v2/internal/protocol/hadoop_hdfs"
	krb "github.com/jcmturner/gokrb5/v8/client"
	"google.golang.org/protobuf/proto"
)

const (
	rpcVersion            byte = 0x09
	serviceClass          byte = 0x0
	noneAuthProtocol      byte = 0x0
	saslAuthProtocol      byte = 0xdf
	protocolClass              = "org.apache.hadoop.hdfs.protocol.ClientProtocol"
	protocolClassVersion       = 1
	handshakeCallID            = -3
	standbyExceptionClass      = "org.apache.hadoop.ipc.StandbyException"
)

// leaseRenewInterval is a variable so tests can shorten it.
var leaseRenewInterval = 1 * time.Second

const (
	backoffDuration = 5 * time.Second

	DefaultDialTimeout    = 30 * time.Second
	DefaultTCPUserTimeout = 30 * time.Second
	DefaultRequestTimeout = 30 * time.Second
	// The TCP keep-alive probe interval used for idle namenode connections.
	keepAliveInterval = 15 * time.Second
)

// NamenodeConnection represents an open connection to a namenode.
type NamenodeConnection struct {
	ClientID   []byte
	ClientName string
	User       string

	currentRequestID int32

	kerberosClient               *krb.Client
	kerberosServicePrincipleName string
	kerberosRealm                string

	// Use SSL
	TLS bool
	// if TLS is set then also set the following parameters
	RootCABundle      string
	ClientCertificate string
	ClientKey         string
	TLSServerName     string

	baseDialFunc      func(ctx context.Context, network, addr string) (net.Conn, error)
	dialFunc          func(ctx context.Context, network, addr string) (net.Conn, error)
	dialTimeout       time.Duration
	tcpUserTimeout    time.Duration
	maxResponseLength uint32
	requestTimeout    time.Duration
	conn              net.Conn
	host              *namenodeHost
	hostList          []*namenodeHost
	transport         transport

	reqLock   chan struct{}
	connMu    sync.Mutex
	closeOnce sync.Once
	closeErr  error
	closed    atomic.Bool
	done      chan struct{}
	renewDone chan struct{}

	// openWriters counts this client's files under lease; leases are renewed only while it is above zero.
	openWriters atomic.Int64
	epoch       atomic.Pointer[rpcEpoch]
}

// NamenodeConnectionOptions represents the configurable options available
// for a NamenodeConnection.
type NamenodeConnectionOptions struct {
	// Addresses specifies the namenode(s) to connect to.
	Addresses []string
	// User specifies which HDFS user the client will act as. It is required
	// unless kerberos authentication is enabled, in which case it is overridden
	// by the username set in KerberosClient.
	User string
	// DialFunc is used to connect to the namenodes. If nil, then
	// (&net.Dialer{}).DialContext is used.
	DialFunc func(ctx context.Context, network, addr string) (net.Conn, error)
	// KerberosClient is used to connect to kerberized HDFS clusters. If provided,
	// the NamenodeConnection will always mutually athenticate when connecting
	// to the namenode(s).
	KerberosClient *krb.Client
	// KerberosServicePrincipleName specifiesthe Service Principle Name
	// (<SERVICE>/<FQDN>) for the namenode(s). Like in the
	// dfs.namenode.kerberos.principal property of core-site.xml, the special
	// string '_HOST' can be substituted for the hostname in a multi-namenode
	// setup (for example: 'nn/_HOST@EXAMPLE.COM'). It is required if
	// KerberosClient is provided.
	KerberosServicePrincipleName string

	// Use SSL
	TLS               bool // if TLS is set then also set the following parameters
	RootCABundle      string
	ClientCertificate string
	ClientKey         string
	// TLSServerName overrides the certificate identity when dialing through a proxy.
	TLSServerName string

	// Bounds how long establishing a TCP (and TLS) connection to a namenode may take.
	DialTimeout time.Duration
	// Bounds how long transmitted data may remain unacknowledged before aborting the connection.
	// Only applied on platforms that support TCP_USER_TIMEOUT.
	TCPUserTimeout time.Duration
	// Bounds the size of one response in bytes; zero selects DefaultMaxResponseLength.
	MaxResponseLength uint32
	// RequestTimeout bounds a complete RPC, including queueing and failover.
	// Zero selects DefaultRequestTimeout; a negative value disables the default.
	RequestTimeout time.Duration
}

func effectiveTimeout(configured, def time.Duration) time.Duration {
	if configured == 0 {
		return def
	}
	if configured < 0 {
		return 0
	}
	return configured
}

type namenodeHost struct {
	address     string
	lastError   error
	lastErrorAt time.Time
}

// NewNamenodeConnectionWithOptions creates a new connection to a namenode with
// the given options and performs an initial handshake.
func NewNamenodeConnection(options NamenodeConnectionOptions) (*NamenodeConnection, error) {
	if len(options.Addresses) == 0 {
		return nil, errors.New("no namenode addresses specified")
	}
	// Build the list of hosts to be used for failover.
	hostList := make([]*namenodeHost, len(options.Addresses))
	for i, addr := range options.Addresses {
		hostList[i] = &namenodeHost{address: addr}
	}

	var user, realm string
	user = options.User
	if options.KerberosClient != nil {
		creds := options.KerberosClient.Credentials
		user = creds.UserName()
		realm = creds.Realm()
	} else if user == "" {
		return nil, errors.New("user not specified")
	}

	// The ClientID is reused here both in the RPC headers (which requires a
	// "globally unique" ID) and as the "client name" in various requests.
	clientId := newClientID()
	c := &NamenodeConnection{
		ClientID:   clientId,
		ClientName: "GO-HopsFS-" + string(clientId),
		User:       user,

		kerberosClient:               options.KerberosClient,
		kerberosServicePrincipleName: options.KerberosServicePrincipleName,
		kerberosRealm:                realm,

		TLS:               options.TLS,
		RootCABundle:      options.RootCABundle,
		ClientCertificate: options.ClientCertificate,
		ClientKey:         options.ClientKey,
		TLSServerName:     options.TLSServerName,

		dialFunc:       options.DialFunc,
		baseDialFunc:   options.DialFunc,
		dialTimeout:    effectiveTimeout(options.DialTimeout, DefaultDialTimeout),
		tcpUserTimeout: effectiveTimeout(options.TCPUserTimeout, DefaultTCPUserTimeout),
		hostList:       hostList,

		maxResponseLength: options.MaxResponseLength,
		requestTimeout:    effectiveTimeout(options.RequestTimeout, DefaultRequestTimeout),

		done:    make(chan struct{}),
		reqLock: make(chan struct{}, 1),
	}
	c.transport = &basicTransport{clientID: clientId, maxResponseLength: options.MaxResponseLength, epoch: c.epochSec}

	if options.TLS {
		c.dialFunc = c.tlsDialFunction
	} else if c.dialFunc == nil {
		c.dialFunc = c.newDialer().DialContext
	}

	err := c.resolveConnection(context.Background())
	if err != nil {
		c.Close()
		return nil, err
	}

	// Periodically renew any file leases.
	c.renewDone = make(chan struct{})
	interval := leaseRenewInterval
	go func() {
		defer close(c.renewDone)
		c.renewLeases(interval)
	}()

	return c, nil
}

func (c *NamenodeConnection) resolveConnection(ctx context.Context) error {
	if c.closed.Load() {
		return io.ErrClosedPipe
	}
	if c.conn != nil {
		return nil
	}

	var err error
	if c.host != nil {
		err = c.host.lastError
	}

	for _, host := range c.hostList {
		if err := ctx.Err(); err != nil {
			return err
		}
		if time.Since(host.lastErrorAt) < backoffDuration {
			continue
		}

		c.host = host
		deadline := c.connectDeadline()
		if limit, ok := ctx.Deadline(); ok && (deadline.IsZero() || limit.Before(deadline)) {
			deadline = limit
		}
		var conn net.Conn
		conn, err = c.dial(ctx, host.address, deadline)
		if err != nil {
			c.markFailure(err)
			continue
		}
		c.connMu.Lock()
		if c.closed.Load() {
			c.connMu.Unlock()
			conn.Close()
			return io.ErrClosedPipe
		}
		c.conn = conn
		c.connMu.Unlock()

		err = c.handshake(ctx, deadline)
		if err != nil {
			c.markFailure(err)
			continue
		}

		break
	}

	if c.conn == nil {
		return fmt.Errorf("no available namenodes: %s", err)
	}

	return nil
}

// newDialer returns the dialer used for namenode connections when no custom
// DialFunc is supplied.
func (c *NamenodeConnection) newDialer() *net.Dialer {
	d := &net.Dialer{
		Timeout:   c.dialTimeout,
		KeepAlive: keepAliveInterval,
	}
	if c.tcpUserTimeout > 0 {
		// nil on platforms without TCP_USER_TIMEOUT.
		d.Control = tcpUserTimeoutControl(c.tcpUserTimeout)
	}
	return d
}

func (c *NamenodeConnection) connectDeadline() time.Time {
	if c.dialTimeout <= 0 {
		return time.Time{}
	}
	return time.Now().Add(c.dialTimeout)
}

// dial connects to address, giving up at deadline unless it is zero.
func (c *NamenodeConnection) dial(ctx context.Context, address string, deadline time.Time) (net.Conn, error) {
	if !deadline.IsZero() {
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, deadline)
		defer cancel()
	}
	return c.dialFunc(ctx, "tcp", address)
}

func (c *NamenodeConnection) handshake(ctx context.Context, deadline time.Time) error {
	if err := c.setDeadline(ctx, deadline); err != nil {
		return fmt.Errorf("setting handshake deadline: %w", err)
	}
	err := c.doNamenodeHandshake()
	if clearErr := c.conn.SetDeadline(time.Time{}); clearErr != nil && err == nil {
		err = fmt.Errorf("clearing handshake deadline: %w", clearErr)
	}
	return err
}

func (c *NamenodeConnection) setDeadline(ctx context.Context, deadline time.Time) error {
	// A cancelled request's watcher must not have its interrupt cleared by setup.
	c.connMu.Lock()
	defer c.connMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	return c.conn.SetDeadline(deadline)
}

func (c *NamenodeConnection) markFailure(err error) {
	c.connMu.Lock()
	defer c.connMu.Unlock()
	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
	}
	c.host.lastError = err
	c.host.lastErrorAt = time.Now()
}

// Execute performs an rpc call. It does this by sending req over the wire and
// unmarshaling the result into resp.
func (c *NamenodeConnection) Execute(method string, req proto.Message, resp proto.Message) error {
	return c.ExecuteContext(context.Background(), method, req, resp)
}

// ExecuteContext performs an RPC within ctx and the configured request timeout.
func (c *NamenodeConnection) ExecuteContext(ctx context.Context, method string, req proto.Message, resp proto.Message) error {
	var cancel context.CancelFunc
	if c.requestTimeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, c.requestTimeout)
	} else {
		ctx, cancel = context.WithCancel(ctx)
	}
	defer cancel()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.done:
		return io.ErrClosedPipe
	case c.reqLock <- struct{}{}:
	}
	defer func() { <-c.reqLock }()
	finished := make(chan struct{})
	watcherDone := make(chan struct{})
	go func() {
		defer close(watcherDone)
		select {
		case <-finished:
			return
		case <-ctx.Done():
		case <-c.done:
			cancel()
		}
		c.connMu.Lock()
		if c.conn != nil {
			c.conn.SetDeadline(time.Now())
		}
		c.connMu.Unlock()
	}()
	defer func() {
		close(finished)
		<-watcherDone
		if c.conn != nil {
			c.conn.SetDeadline(time.Time{})
		}
	}()
	return c.execute(ctx, method, req, resp)
}

func (c *NamenodeConnection) execute(ctx context.Context, method string, req proto.Message, resp proto.Message) error {

	c.currentRequestID++
	requestID := c.currentRequestID

	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := c.resolveConnection(ctx)
		if err != nil {
			return err
		}
		deadline, _ := ctx.Deadline()
		if err := c.setDeadline(ctx, deadline); err != nil {
			c.markFailure(err)
			return err
		}

		err = c.transport.writeRequest(c.conn, method, requestID, req)
		if err != nil {
			c.markFailure(err)
			if ctx.Err() != nil {
				return ctx.Err()
			}
			continue
		}

		err = c.transport.readResponse(c.conn, method, requestID, resp)
		if err != nil {
			if ctx.Err() != nil {
				c.markFailure(err)
				return ctx.Err()
			}
			if nerr, ok := err.(*NamenodeError); ok {
				// The namenode answered, so the connection itself is healthy.
				// Only retry on a standby exception.
				if nerr.exception == standbyExceptionClass || nerr.exception == "org.apache.hadoop.ipc.NotALeaderException" || strings.HasPrefix(nerr.message, "org.apache.hadoop.ipc.NotALeaderException") {
					c.markFailure(err)
					continue
				}
				if nerr.fatal {
					c.markFailure(err)
				}
				return err
			}

			// Anything else (EOF, reset, user timeout, framing error) means
			// the connection can no longer be trusted.
			c.markFailure(err)
			return err
		}

		break
	}

	return nil
}

// A handshake packet:
// +-----------------------------------------------------------+
// |  Header, 4 bytes ("hrpc")                                 |
// +-----------------------------------------------------------+
// |  Version, 1 byte (default verion 0x09)                    |
// +-----------------------------------------------------------+
// |  RPC service class, 1 byte (0x00)                         |
// +-----------------------------------------------------------+
// |  Auth protocol, 1 byte (Auth method None = 0x00)          |
// +-----------------------------------------------------------+
//
//	If the auth protocol is something other than 'none', the authentication
//	handshake happens here. Otherwise, everything can be sent as one packet.
//
// +-----------------------------------------------------------+
// |  uint32 length of the next two parts                      |
// +-----------------------------------------------------------+
// |  varint length + RpcRequestHeaderProto                    |
// +-----------------------------------------------------------+
// |  varint length + IpcConnectionContextProto                |
// +-----------------------------------------------------------+
func (c *NamenodeConnection) doNamenodeHandshake() error {
	authProtocol := noneAuthProtocol
	kerberos := false
	if c.kerberosClient != nil {
		authProtocol = saslAuthProtocol
		kerberos = true
	}

	rpcHeader := []byte{
		0x68, 0x72, 0x70, 0x63, // "hrpc"
		rpcVersion, serviceClass, authProtocol,
	}

	_, err := c.conn.Write(rpcHeader)
	if err != nil {
		return err
	}

	if kerberos {
		err = c.doKerberosHandshake()
		if err != nil {
			return fmt.Errorf("SASL handshake: %s", err)
		}
	}

	rrh := newRPCRequestHeader(handshakeCallID, c.ClientID)
	rrh.Epoch = proto.Int64(c.epochSec())
	cc := newConnectionContext(c.User, c.kerberosRealm)
	packet, err := makeRPCPacket(rrh, cc)
	if err != nil {
		return err
	}

	_, err = c.conn.Write(packet)
	return err
}

// AcquireLease records that a file of this client was opened for writing, so its lease must be renewed.
func (c *NamenodeConnection) AcquireLease() {
	c.openWriters.Add(1)
}

// ReleaseLease records that a file recorded with AcquireLease was completed.
func (c *NamenodeConnection) ReleaseLease() {
	c.openWriters.Add(-1)
}

// OpenWriters reports how many of this client's files are under lease.
func (c *NamenodeConnection) OpenWriters() int64 {
	return c.openWriters.Load()
}

// renewLeases periodically renews the client's leases while it has files open for writing.
//
// A client with no open writers holds no lease, so it sends nothing. Renewing anyway kept every
// idle reader calling the namenode each second, and redialling it after each failure for as long
// as the process lived.
func (c *NamenodeConnection) renewLeases(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if c.openWriters.Load() <= 0 {
				continue
			}
			req := &hdfs.RenewLeaseRequestProto{ClientName: proto.String(c.ClientName)}
			resp := &hdfs.RenewLeaseResponseProto{}

			// Ignore any errors.
			c.Execute("renewLease", req, resp)
		case <-c.done:
			return
		}
	}
}

// Close terminates all underlying socket connections to remote server.
func (c *NamenodeConnection) Close() error {
	c.closeOnce.Do(func() {
		c.closed.Store(true)
		if c.done != nil {
			close(c.done)
		}
		c.connMu.Lock()
		defer c.connMu.Unlock()
		if c.conn != nil {
			c.closeErr = c.conn.Close()
		}
	})
	if c.renewDone != nil {
		<-c.renewDone
	}
	return c.closeErr
}

func (c *NamenodeConnection) tlsDialFunction(ctx context.Context, network, address string) (net.Conn, error) {
	// Load client's certificate(including the intermediate) and private key
	clientCert, err := tls.LoadX509KeyPair(c.ClientCertificate, c.ClientKey)
	if err != nil {
		return nil, err
	}

	// Load certificate of the CA who signed server's certificate
	pemServerCA, err := ioutil.ReadFile(c.RootCABundle)
	if err != nil {
		return nil, err
	}

	certChain := decodePem(pemServerCA)

	config := &tls.Config{}

	config.RootCAs = x509.NewCertPool()
	for _, cert := range certChain.Certificate {
		x509Cert, err := x509.ParseCertificate(cert)
		if err != nil {
			return nil, fmt.Errorf("parsing root CA bundle %s: %w", c.RootCABundle, err)
		}
		config.RootCAs.AddCert(x509Cert)
	}

	if len(certChain.Certificate) == 0 {
		return nil, fmt.Errorf("root CA bundle %s contains no certificates", c.RootCABundle)
	}
	config.Certificates = []tls.Certificate{clientCert}
	config.MinVersion = tls.VersionTLS12
	config.ServerName = c.TLSServerName
	if config.ServerName == "" {
		host, _, err := net.SplitHostPort(address)
		if err != nil {
			return nil, err
		}
		config.ServerName = host
	}
	dial := c.baseDialFunc
	if dial == nil {
		dial = c.newDialer().DialContext
	}
	raw, err := dial(ctx, network, address)
	if err != nil {
		return nil, err
	}
	conn := tls.Client(raw, config)
	if err := conn.HandshakeContext(ctx); err != nil {
		raw.Close()
		return nil, err
	}
	return conn, nil
}

func decodePem(certInput []byte) tls.Certificate {
	var cert tls.Certificate
	certPEMBlock := certInput
	var certDERBlock *pem.Block
	for {
		certDERBlock, certPEMBlock = pem.Decode(certPEMBlock)
		if certDERBlock == nil {
			break
		}
		if certDERBlock.Type == "CERTIFICATE" {
			cert.Certificate = append(cert.Certificate, certDERBlock.Bytes)
		}
	}
	return cert
}

func newRPCRequestHeader(id int32, clientID []byte) *hadoop.RpcRequestHeaderProto {
	epoch := getRpcEpochSec()
	return &hadoop.RpcRequestHeaderProto{
		RpcKind:  hadoop.RpcKindProto_RPC_PROTOCOL_BUFFER.Enum(),
		RpcOp:    hadoop.RpcRequestHeaderProto_RPC_FINAL_PACKET.Enum(),
		CallId:   proto.Int32(id),
		ClientId: clientID,
		Epoch:    &epoch,
	}
}

func newRequestHeader(methodName string) *hadoop.RequestHeaderProto {
	return &hadoop.RequestHeaderProto{
		MethodName:                 proto.String(methodName),
		DeclaringClassProtocolName: proto.String(protocolClass),
		ClientProtocolVersion:      proto.Uint64(uint64(protocolClassVersion)),
	}
}

func newConnectionContext(user, kerberosRealm string) *hadoop.IpcConnectionContextProto {
	if kerberosRealm != "" {
		user = user + "@" + kerberosRealm
	}

	return &hadoop.IpcConnectionContextProto{
		UserInfo: &hadoop.UserInformationProto{
			EffectiveUser: proto.String(user),
		},
		Protocol: proto.String(protocolClass),
	}
}

type rpcEpoch struct {
	reported int64
	at       time.Time
}

var defaultEpoch atomic.Pointer[rpcEpoch]

func SetEpoch(reportTime time.Time, epoch int64) {
	defaultEpoch.Store(&rpcEpoch{reported: epoch, at: reportTime})
}

func getRpcEpochSec() int64 {
	return epochSec(defaultEpoch.Load())
}

func epochSec(epoch *rpcEpoch) int64 {
	if epoch == nil || epoch.reported == 0 {
		return 0
	}
	return (epoch.reported + time.Since(epoch.at).Milliseconds()) / 1000
}

// SetEpoch sets this connection's server time in milliseconds.
func (c *NamenodeConnection) SetEpoch(reportTime time.Time, epoch int64) {
	c.epoch.Store(&rpcEpoch{reported: epoch, at: reportTime})
}

func (c *NamenodeConnection) epochSec() int64 {
	return epochSec(c.epoch.Load())
}
