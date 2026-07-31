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
	S3CredentialPath string
	RaftNodeCount    int
	TLSEnabled       bool
	AllowInsecureDev bool
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
	if cfg.S3CredentialPath == "" {
		errs = append(errs, "production S3 credential source is required")
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
