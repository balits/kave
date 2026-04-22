package mtls

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/hashicorp/raft"
)

// Options contains tls information, and
// gets injected through config.Config
type Options struct {
	CertFile string `json:"cert_file"`
	KeyFile  string `json:"key_file"`
	CaFile   string `json:"ca_file"`
}

func (o *Options) Check() error {
	if len(o.CertFile) == 0 {
		return errors.New("cert_file not set")
	}
	if len(o.KeyFile) == 0 {
		return errors.New("key_fileis not set")
	}
	if len(o.CaFile) == 0 {
		return errors.New("ca_file is not set")
	}
	return nil
}

// StreamLayer implements raft.StreamLayer that gets
// injected into raft.Transport, which allows nodes
// to use mTLS when communicating with eachoter.
type StreamLayer struct {
	ln     net.Listener
	addr   raft.ServerAddress
	tlsCfg *tls.Config
}

func NewStreamLayer(listenAddr string, advertiseAddr raft.ServerAddress, certFile, keyFile, caFile string) (raft.StreamLayer, error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("failed to load raft TLS cert/key: %w", err)
	}

	caPEM, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("failed to read internal raft CA cert: %w", err)
	}
	caPool := x509.NewCertPool()
	if !caPool.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("failed to parse internal raft CA cert")
	}

	tlsCfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      caPool,
		ClientCAs:    caPool,
		ClientAuth:   tls.RequireAndVerifyClientCert, // this is what makes it mutual TLS
		MinVersion:   tls.VersionTLS13,
	}

	tcpListener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to bind raft listener on %s: %w", listenAddr, err)
	}
	tlsListener := tls.NewListener(tcpListener, tlsCfg) // go through tls automatically

	return &StreamLayer{
		ln:     tlsListener,
		addr:   advertiseAddr,
		tlsCfg: tlsCfg,
	}, nil
}

// from net: Accept waits for and returns the next connection to the listener.
func (l *StreamLayer) Accept() (net.Conn, error) { return l.ln.Accept() }

// from net:Close closes the listener.
// Any blocked Accept operations will be unblocked and return errors.
func (l *StreamLayer) Close() error { return l.ln.Close() }

// from net:Addr returns the listener's network address.
func (l *StreamLayer) Addr() net.Addr { return l.ln.Addr() }

// from hashicorp/raft: Dial is used to create a new outgoing connection
func (sl *StreamLayer) Dial(address raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	host, _, err := net.SplitHostPort(string(address))
	if err != nil {
		return nil, fmt.Errorf("failed to parse peer address %q: %w", address, err)
	}

	// clone -> no data race when dialing
	dialCfg := sl.tlsCfg.Clone()
	// mathcing *.kave-headless.kave.svc.cluster.local as SAN
	dialCfg.ServerName = host

	dialer := &net.Dialer{Timeout: timeout}
	return tls.DialWithDialer(dialer, "tcp", string(address), dialCfg)
}

const (
	CertFile = "/tls/raft/tls.crt"
	KeyFile  = "/tls/raft/tls.key"
	CaFile   = "/tls/raft/ca.crt"
)
