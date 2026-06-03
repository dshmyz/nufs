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

func SaveCredentials(cfgDir string, c *Credentials) error {
	if err := os.MkdirAll(cfgDir, 0700); err != nil {
		return err
	}
	data, err := json.Marshal(c)
	if err != nil {
		return err
	}
	return os.WriteFile(path.Join(cfgDir, "credentials.json"), data, 0600)
}
