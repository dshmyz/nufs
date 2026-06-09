package tlsutil

import (
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

// TestTLSEndToEnd verifies that ServerConfig and ClientConfig produce
// a working TLS handshake between a listener and a dialer.
func TestTLSEndToEnd(t *testing.T) {
	dir := t.TempDir()

	// Generate a self-signed CA + server cert + client cert
	caCertPEM, caKeyPEM := generateCA(t)
	serverCertPEM, serverKeyPEM := generateCert(t, caCertPEM, caKeyPEM, "server")
	clientCertPEM, clientKeyPEM := generateCert(t, caCertPEM, caKeyPEM, "client")

	caPath := filepath.Join(dir, "ca.pem")
	serverCertPath := filepath.Join(dir, "server.pem")
	serverKeyPath := filepath.Join(dir, "server-key.pem")
	clientCertPath := filepath.Join(dir, "client.pem")
	clientKeyPath := filepath.Join(dir, "client-key.pem")

	os.WriteFile(caPath, caCertPEM, 0644)
	os.WriteFile(serverCertPath, serverCertPEM, 0644)
	os.WriteFile(serverKeyPath, serverKeyPEM, 0600)
	os.WriteFile(clientCertPath, clientCertPEM, 0644)
	os.WriteFile(clientKeyPath, clientKeyPEM, 0600)

	t.Run("server_and_client_mtls", func(t *testing.T) {
		// Server config with mTLS
		srvCfg, err := ServerConfig(Config{
			CertFile: serverCertPath,
			KeyFile:  serverKeyPath,
			CAFile:   caPath,
		})
		if err != nil {
			t.Fatalf("ServerConfig: %v", err)
		}
		if srvCfg == nil {
			t.Fatal("expected non-nil TLS config")
		}

		// Client config with mTLS
		cliCfg, err := ClientConfig(Config{
			CertFile: clientCertPath,
			KeyFile:  clientKeyPath,
			CAFile:   caPath,
		})
		if err != nil {
			t.Fatalf("ClientConfig: %v", err)
		}

		// Start a TLS listener
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen: %v", err)
		}
		tlsLn := NewListener(ln, srvCfg)
		defer tlsLn.Close()

		done := make(chan struct{})
		go func() {
			defer close(done)
			conn, err := tlsLn.Accept()
			if err != nil {
				t.Errorf("accept: %v", err)
				return
			}
			defer conn.Close()
			buf := make([]byte, 1)
			if _, err := conn.Read(buf); err != nil {
				t.Errorf("server read: %v", err)
				return
			}
			if _, err := conn.Write(buf); err != nil {
				t.Errorf("server write: %v", err)
			}
		}()

		conn, err := tls.Dial("tcp", ln.Addr().String(), cliCfg)
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		defer conn.Close()

		state := conn.ConnectionState()
		if !state.HandshakeComplete {
			t.Fatal("handshake not complete")
		}

		if _, err := conn.Write([]byte{42}); err != nil {
			t.Fatalf("client write: %v", err)
		}
		buf := make([]byte, 1)
		if _, err := conn.Read(buf); err != nil {
			t.Fatalf("client read: %v", err)
		}
		if buf[0] != 42 {
			t.Fatalf("echo mismatch: got %d, want 42", buf[0])
		}
		<-done
	})

	t.Run("server_only_no_mtls", func(t *testing.T) {
		srvCfg, err := ServerConfig(Config{
			CertFile: serverCertPath,
			KeyFile:  serverKeyPath,
		})
		if err != nil {
			t.Fatalf("ServerConfig: %v", err)
		}

		cliCfg, err := ClientConfig(Config{
			CAFile: caPath,
		})
		if err != nil {
			t.Fatalf("ClientConfig: %v", err)
		}

		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen: %v", err)
		}
		tlsLn := NewListener(ln, srvCfg)

		serverDone := make(chan struct{})
		go func() {
			defer close(serverDone)
			conn, err := tlsLn.Accept()
			if err != nil {
				return
			}
			defer conn.Close()
			// Echo back a byte
			buf := make([]byte, 1)
			if _, err := conn.Read(buf); err != nil {
				return
			}
			conn.Write(buf)
		}()

		conn, err := tls.Dial("tcp", ln.Addr().String(), cliCfg)
		if err != nil {
			tlsLn.Close()
			t.Fatalf("dial: %v", err)
		}
		defer conn.Close()
		defer tlsLn.Close()

		// Verify handshake completed
		state := conn.ConnectionState()
		if !state.HandshakeComplete {
			t.Fatal("handshake not complete")
		}

		if _, err := conn.Write([]byte{7}); err != nil {
			t.Fatalf("write: %v", err)
		}
		buf := make([]byte, 1)
		if _, err := conn.Read(buf); err != nil {
			t.Fatalf("read: %v", err)
		}
		if buf[0] != 7 {
			t.Fatalf("echo mismatch: got %d, want 7", buf[0])
		}
		<-serverDone
	})

	t.Run("disabled_returns_nil", func(t *testing.T) {
		cfg, err := ServerConfig(Config{})
		if err != nil {
			t.Fatalf("ServerConfig: %v", err)
		}
		if cfg != nil {
			t.Fatal("expected nil when TLS is disabled")
		}
	})

	t.Run("enabled_flag", func(t *testing.T) {
		empty := Config{}
		if empty.Enabled() {
			t.Fatal("empty config should not be enabled")
		}
		withCerts := Config{CertFile: "a", KeyFile: "b"}
		if !withCerts.Enabled() {
			t.Fatal("config with cert+key should be enabled")
		}
	})

	t.Run("skip_verify", func(t *testing.T) {
		cliCfg, err := ClientConfig(Config{SkipVerify: true})
		if err != nil {
			t.Fatalf("ClientConfig: %v", err)
		}
		if !cliCfg.InsecureSkipVerify {
			t.Fatal("expected InsecureSkipVerify=true")
		}
	})

	t.Run("require_client_cert", func(t *testing.T) {
		cfg, err := ServerConfig(Config{
			CertFile:          serverCertPath,
			KeyFile:           serverKeyPath,
			CAFile:            caPath,
			RequireClientCert: true,
		})
		if err != nil {
			t.Fatalf("ServerConfig: %v", err)
		}
		if cfg.ClientAuth != tls.RequireAndVerifyClientCert {
			t.Fatalf("expected required client cert auth, got %v", cfg.ClientAuth)
		}
	})
}

// --- Certificate generation helpers ---

func generateCA(t *testing.T) (certPEM, keyPEM []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate CA key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "Test CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
	}
	certDER, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create CA cert: %v", err)
	}
	certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})

	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshal CA key: %v", err)
	}
	keyPEM = pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	return certPEM, keyPEM
}

func generateCert(t *testing.T, caCertPEM, caKeyPEM []byte, cn string) (certPEM, keyPEM []byte) {
	t.Helper()
	caBlock, _ := pem.Decode(caCertPEM)
	if caBlock == nil {
		t.Fatal("failed to decode CA cert PEM")
	}
	ca, err := x509.ParseCertificate(caBlock.Bytes)
	if err != nil {
		t.Fatalf("parse CA cert: %v", err)
	}

	keyBlock, _ := pem.Decode(caKeyPEM)
	if keyBlock == nil {
		t.Fatal("failed to decode CA key PEM")
	}
	caKey, err := x509.ParseECPrivateKey(keyBlock.Bytes)
	if err != nil {
		t.Fatalf("parse CA key: %v", err)
	}

	certKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate cert key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1)},
		DNSNames:     []string{"localhost"},
	}
	certDER, err := x509.CreateCertificate(rand.Reader, tmpl, ca, &certKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create cert: %v", err)
	}
	certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})

	keyDER, err := x509.MarshalECPrivateKey(certKey)
	if err != nil {
		t.Fatalf("marshal cert key: %v", err)
	}
	keyPEM = pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	return certPEM, keyPEM
}
