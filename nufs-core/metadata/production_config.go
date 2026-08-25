package metadata

import (
	"errors"
	"fmt"
	"strings"
)

type RuntimeMode string

const (
	RuntimeDev        RuntimeMode = "dev"
	RuntimeProduction RuntimeMode = "production"
)

type ProductionValidationConfig struct {
	Mode             RuntimeMode
	JWTSecret        string
	RaftNodeCount    int
	TLSEnabled       bool
	AllowInsecureDev bool
	// TokenSigningKey is the HMAC key metad uses to sign mount auth tokens.
	// Required in production: without it the fuse cannot authenticate, and a
	// dev-default value would allow forging tokens.
	TokenSigningKey string
	// CredentialSecretKey is the 32-byte hex key metad uses to seal registry
	// secrets for the S3 gateway credential sync. Required in production:
	// without it secrets are stored hash-only and the S3 gateway cannot
	// authenticate any client. It is not a dev default; an empty value is
	// rejected here.
	CredentialSecretKey string
}

func ValidateProductionConfig(cfg ProductionValidationConfig) error {
	if cfg.Mode != RuntimeProduction {
		if cfg.AllowInsecureDev {
			return nil
		}
		return errors.New("non-production mode requires AllowInsecureDev")
	}

	var errs []string
	if cfg.JWTSecret == "" ||
		strings.Contains(cfg.JWTSecret, "dev-secret") ||
		strings.Contains(cfg.JWTSecret, "change-in-production") {
		errs = append(errs, "production JWT secret is empty or uses a dev default")
	}
	if cfg.TokenSigningKey == "" ||
		strings.Contains(cfg.TokenSigningKey, "dev-token-key") ||
		strings.Contains(cfg.TokenSigningKey, "change-in-production") {
		errs = append(errs, "production token signing key is empty or uses a dev default")
	}
	if cfg.CredentialSecretKey == "" {
		errs = append(errs, "production credential secret key is required (S3 gateway credential sync)")
	}
	if cfg.RaftNodeCount < 3 {
		errs = append(errs, "production Raft requires at least 3 nodes")
	}
	if !cfg.TLSEnabled {
		errs = append(errs, "production TLS must be enabled")
	}
	if len(errs) > 0 {
		return fmt.Errorf("invalid production config: %s", strings.Join(errs, "; "))
	}
	return nil
}
