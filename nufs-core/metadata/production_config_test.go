package metadata

import "testing"

func TestValidateProductionConfigRejectsDevSecret(t *testing.T) {
	err := ValidateProductionConfig(ProductionValidationConfig{
		Mode:             RuntimeProduction,
		JWTSecret:        "dev-secret-change-in-production",
		S3CredentialPath: "/etc/nufs/s3.yaml",
		RaftNodeCount:    3,
		TLSEnabled:       true,
	})
	if err == nil {
		t.Fatal("expected production config with dev secret to fail")
	}
}

func TestValidateProductionConfigRejectsSingleNodeRaft(t *testing.T) {
	err := ValidateProductionConfig(ProductionValidationConfig{
		Mode:             RuntimeProduction,
		JWTSecret:        "a-long-production-secret-value",
		S3CredentialPath: "/etc/nufs/s3.yaml",
		RaftNodeCount:    1,
		TLSEnabled:       true,
	})
	if err == nil {
		t.Fatal("expected single-node production raft to fail")
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
