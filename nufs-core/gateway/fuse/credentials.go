package fuse

import (
	"encoding/json"
	"os"
	"path"
)

type Credentials struct {
	AccessKey string `json:"access_key,omitempty"`
	SecretKey string `json:"secret_key,omitempty"`
	Endpoint  string `json:"metadata_endpoint,omitempty"`
}

// LoadCredentials reads the mount credential from credentials.json (if
// present) and META_ACCESS_KEY/META_SECRET_KEY/META_ENDPOINT env vars. Env
// wins over the file. There is intentionally no SaveCredentials counterpart:
// the accessKey/secretKey are inputs the operator injects (file or env), and
// the signed token exchanged from them is deliberately kept in memory only —
// persisting it would leave an expired-token file that silently goes stale.
// On remount the in-memory secret is re-exchanged for a fresh token instead.
func LoadCredentials(cfgDir string) *Credentials {
	c := &Credentials{}
	data, err := os.ReadFile(path.Join(cfgDir, "credentials.json"))
	if err == nil {
		json.Unmarshal(data, c)
	}
	if v := os.Getenv("META_ACCESS_KEY"); v != "" {
		c.AccessKey = v
	}
	if v := os.Getenv("META_SECRET_KEY"); v != "" {
		c.SecretKey = v
	}
	if v := os.Getenv("META_ENDPOINT"); v != "" {
		c.Endpoint = v
	}
	return c
}
