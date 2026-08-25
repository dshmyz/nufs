package metadata

import "testing"

func TestValidateProductionConfigRejectsDevSecret(t *testing.T) {
	err := ValidateProductionConfig(ProductionValidationConfig{
		Mode:                RuntimeProduction,
		JWTSecret:           "dev-secret-change-in-production",
		RaftNodeCount:       3,
		TLSEnabled:          true,
		TokenSigningKey:     "a-long-production-token-key",
		CredentialSecretKey: "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
	})
	if err == nil {
		t.Fatal("expected production config with dev secret to fail")
	}
}

func TestValidateProductionConfigRejectsSingleNodeRaft(t *testing.T) {
	err := ValidateProductionConfig(ProductionValidationConfig{
		Mode:                RuntimeProduction,
		JWTSecret:           "a-long-production-secret-value",
		RaftNodeCount:       1,
		TLSEnabled:          true,
		TokenSigningKey:     "a-long-production-token-key",
		CredentialSecretKey: "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
	})
	if err == nil {
		t.Fatal("expected single-node production raft to fail")
	}
}

func TestValidateProductionConfigRejectsMissingTokenSigningKey(t *testing.T) {
	err := ValidateProductionConfig(ProductionValidationConfig{
		Mode:                RuntimeProduction,
		JWTSecret:           "a-long-production-secret-value",
		RaftNodeCount:       3,
		TLSEnabled:          true,
		TokenSigningKey:     "dev-token-key-change-in-production",
		CredentialSecretKey: "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
	})
	if err == nil {
		t.Fatal("expected production config with dev token key to fail")
	}
}

func TestValidateProductionConfigAllowsExplicitDevMode(t *testing.T) {
	err := ValidateProductionConfig(ProductionValidationConfig{
		Mode:             RuntimeDev,
		JWTSecret:        "dev-secret-change-in-production",
		RaftNodeCount:    1,
		TLSEnabled:       false,
		AllowInsecureDev: true,
	})
	if err != nil {
		t.Fatalf("dev config should pass: %v", err)
	}
}

func TestValidateProductionConfigAllowsValidProduction(t *testing.T) {
	// A fully-configured production metad passes: the S3 gateway credential
	// source is the metad registry itself (seeded via the ops API), so no
	// file-backed credential path is required.
	err := ValidateProductionConfig(ProductionValidationConfig{
		Mode:                RuntimeProduction,
		JWTSecret:           "a-long-production-secret-value",
		RaftNodeCount:       3,
		TLSEnabled:          true,
		TokenSigningKey:     "a-long-production-token-key",
		CredentialSecretKey: "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
	})
	if err != nil {
		t.Fatalf("valid production config should pass: %v", err)
	}
}

func TestValidateProductionConfigRejectsMissingCredentialSecretKey(t *testing.T) {
	err := ValidateProductionConfig(ProductionValidationConfig{
		Mode:            RuntimeProduction,
		JWTSecret:       "a-long-production-secret-value",
		RaftNodeCount:   3,
		TLSEnabled:      true,
		TokenSigningKey: "a-long-production-token-key",
		// CredentialSecretKey deliberately omitted: the S3 gateway credential
		// sync would have nothing to unseal.
	})
	if err == nil {
		t.Fatal("expected production config without credential secret key to fail")
	}
}
