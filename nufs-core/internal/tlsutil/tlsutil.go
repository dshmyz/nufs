// Package tlsutil provides shared TLS configuration helpers for all NUFS
// components (metad, datanode, gateways). It centralises certificate loading,
// mutual-TLS setup, and sensible defaults so each binary only needs to pass
// file paths.
package tlsutil

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"os"
)

// Config holds the TLS parameters shared across all NUFS services.
// All fields are optional; when CertFile and KeyFile are empty the
// service runs in plain-text mode.
type Config struct {
	// CertFile is the path to the TLS certificate (PEM).
	CertFile string
	// KeyFile is the path to the TLS private key (PEM).
	KeyFile string
	// CAFile is the path to the CA certificate used for client
	// verification (mutual TLS). When empty, client certs are
	// not verified.
	CAFile string
	// SkipVerify disables server certificate verification on the
	// client side. Intended only for development / testing.
	SkipVerify bool
	// RequireClientCert forces clients to present a certificate signed by
	// CAFile. When false and CAFile is set, client certificates are verified
	// only if the client presents one.
	RequireClientCert bool
}

// Enabled returns true when both CertFile and KeyFile are set.
func (c Config) Enabled() bool {
	return c.CertFile != "" && c.KeyFile != ""
}

// ServerConfig builds a *tls.Config suitable for a TLS listener.
// It loads the certificate pair and, when CAFile is set, configures client
// certificate verification. Set RequireClientCert to enforce mutual TLS.
func ServerConfig(cfg Config) (*tls.Config, error) {
	if !cfg.Enabled() {
		return nil, nil
	}

	cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("tlsutil: load key pair: %w", err)
	}

	tlsCfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}

	if cfg.CAFile != "" {
		pool, err := loadCACert(cfg.CAFile)
		if err != nil {
			return nil, err
		}
		tlsCfg.ClientCAs = pool
		if cfg.RequireClientCert {
			tlsCfg.ClientAuth = tls.RequireAndVerifyClientCert
		} else {
			tlsCfg.ClientAuth = tls.VerifyClientCertIfGiven
		}
	}

	return tlsCfg, nil
}

// ClientConfig builds a *tls.Config suitable for a TLS client
// (dialer). When the server uses mutual-TLS, set CertFile/KeyFile
// on the caller's Config; when CAFile is set the server certificate
// is verified against that CA. SkipVerify bypasses verification.
func ClientConfig(cfg Config) (*tls.Config, error) {
	tlsCfg := &tls.Config{
		MinVersion: tls.VersionTLS12,
	}

	if cfg.SkipVerify {
		tlsCfg.InsecureSkipVerify = true
		return tlsCfg, nil
	}

	if cfg.CAFile != "" {
		pool, err := loadCACert(cfg.CAFile)
		if err != nil {
			return nil, err
		}
		tlsCfg.RootCAs = pool
	}

	if cfg.CertFile != "" && cfg.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("tlsutil: load client key pair: %w", err)
		}
		tlsCfg.Certificates = []tls.Certificate{cert}
	}

	return tlsCfg, nil
}

func loadCACert(path string) (*x509.CertPool, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("tlsutil: read CA cert: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(data) {
		return nil, fmt.Errorf("tlsutil: no certificates found in %s", path)
	}
	return pool, nil
}

// NewListener wraps an existing net.Listener with TLS using the provided
// *tls.Config. It is a convenience wrapper around tls.NewListener.
func NewListener(ln net.Listener, cfg *tls.Config) net.Listener {
	return tls.NewListener(ln, cfg)
}
