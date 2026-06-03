package s3fs

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path"
	"time"
)

// Config holds all configuration for an S3 FUSE mount.
type Config struct {
	Bucket   string
	BasePath string
	Target   *url.URL

	CacheDir    string
	ScanTTL     time.Duration
	MetricsAddr string
	ReadOnly    bool
	CacheQuota  int64
	UID, GID    uint32
	Mode        os.FileMode
	Insecure    bool
	Debug       bool
}

// AccessConfig is the credential file format.
type AccessConfig struct {
	Version     string `json:"version"`
	AccessKey   string `json:"accessKey"`
	SecretKey   string `json:"secretKey"`
	SecretToken string `json:"secretToken"`
}

// LoadCredentials reads credentials from file, env, or defaults.
func LoadCredentials(cacheDir string) *AccessConfig {
	ac := &AccessConfig{Version: "1"}

	// Try config file first.
	cfgPath := path.Join(cacheDir, "config.json")
	if data, err := os.ReadFile(cfgPath); err == nil {
		_ = json.Unmarshal(data, ac)
	}

	// Environment overrides file.
	if v := os.Getenv("S3FS_ACCESS_KEY"); v != "" {
		ac.AccessKey = v
	}
	if v := os.Getenv("S3FS_SECRET_KEY"); v != "" {
		ac.SecretKey = v
	}
	if v := os.Getenv("S3FS_SECRET_TOKEN"); v != "" {
		ac.SecretToken = v
	}
	return ac
}

// SaveCredentials writes credentials to the config file.
func SaveCredentials(cacheDir string, ac *AccessConfig) error {
	if err := os.MkdirAll(cacheDir, 0700); err != nil {
		return err
	}
	data, err := json.Marshal(ac)
	if err != nil {
		return err
	}
	return os.WriteFile(path.Join(cacheDir, "config.json"), data, 0600)
}

// Option is a functional option for Config.
type Option func(*Config)

func WithBucket(bucket string) Option  { return func(c *Config) { c.Bucket = bucket } }
func WithBasePath(p string) Option     { return func(c *Config) { c.BasePath = p } }
func WithCacheDir(d string) Option     { return func(c *Config) { c.CacheDir = d } }
func WithScanTTL(ttl time.Duration) Option {
	return func(c *Config) { c.ScanTTL = ttl }
}
func WithMetricsAddr(addr string) Option {
	return func(c *Config) { c.MetricsAddr = addr }
}
func WithReadOnly() Option       { return func(c *Config) { c.ReadOnly = true } }
func WithCacheQuota(n int64) Option {
	return func(c *Config) { c.CacheQuota = n }
}
func WithUID(uid uint32) Option  { return func(c *Config) { c.UID = uid } }
func WithGID(gid uint32) Option  { return func(c *Config) { c.GID = gid } }
func WithInsecure() Option       { return func(c *Config) { c.Insecure = true } }
func WithDebug() Option          { return func(c *Config) { c.Debug = true } }

// ParseTarget parses an S3 endpoint URL like "https://s3.amazonaws.com/bucket/prefix".
func ParseTarget(target string) (*url.URL, string, string, error) {
	u, err := url.Parse(target)
	if err != nil {
		return nil, "", "", fmt.Errorf("parse target: %w", err)
	}
	if u.Host == "" {
		return nil, "", "", errors.New("target must include host")
	}
	var bucket, basePath string
	if len(u.Path) > 1 {
		parts := splitPath(u.Path[1:])
		if len(parts) > 0 {
			bucket = parts[0]
		}
		if len(parts) > 1 {
			basePath = path.Join(parts[1:]...)
		}
	}
	if bucket == "" {
		return nil, "", "", errors.New("target must include bucket in path: https://host/bucket[/prefix]")
	}
	return u, bucket, basePath, nil
}

func splitPath(p string) []string {
	if p == "" {
		return nil
	}
	var parts []string
	dir, file := path.Split(p)
	if file != "" {
		parts = append(parts, file)
	}
	for dir != "" && dir != "/" {
		dir, file = path.Split(dir[:len(dir)-1])
		if file != "" {
			parts = append(parts, file)
		}
	}
	// Reverse since we collected from leaf to root.
	for i, j := 0, len(parts)-1; i < j; i, j = i+1, j-1 {
		parts[i], parts[j] = parts[j], parts[i]
	}
	return parts
}

// RemotePath returns the full S3 key for a given filesystem path.
func (c *Config) RemotePath(fsPath string) string {
	return path.Join(c.BasePath, fsPath)
}

// Validate checks the config for sane values.
func (c *Config) Validate() error {
	if c.Bucket == "" {
		return errors.New("bucket not set")
	}
	if c.Target == nil {
		return errors.New("target not set")
	}
	if c.CacheDir == "" {
		return errors.New("cache dir not set")
	}
	if c.ScanTTL <= 0 {
		c.ScanTTL = 60 * time.Second
	}
	if c.MetricsAddr == "" {
		c.MetricsAddr = ":9900"
	}
	if c.Mode == 0 {
		c.Mode = 0660
	}
	return nil
}
