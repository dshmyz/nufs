// Copyright (c) 2021 MinIO, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package minfs

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/minio/minio-go/v7/pkg/credentials"
)

// Config is being used for storge of configuration items
type Config struct {
	bucket   string
	basePath string

	cache       string
	accountID   string
	accessKey   string
	secretKey   string
	secretToken string
	target      *url.URL
	mountpoint  string
	insecure    bool
	debug       bool

	// scanTTL controls how long a directory scan result is cached before
	// a fresh ListObjects call is made. Default is 60 seconds.
	scanTTL time.Duration

	// metricsAddr is the address for the metrics/health HTTP server.
	// An empty string disables the metrics server. Default is ":9900".
	metricsAddr string

	// readOnly disables all write operations (Create, Mkdir, Remove, Rename, Write).
	readOnly bool

	// cacheQuota is the maximum total size in bytes of the local cache directory.
	// A value of 0 means unlimited. When exceeded, the oldest cache files are evicted.
	cacheQuota int64

	uid  uint32
	gid  uint32
	mode os.FileMode
}

// AccessConfig - access credentials and version of `config.json`.
type AccessConfig struct {
	Version     string `json:"version"`
	AccessKey   string `json:"accessKey"`
	SecretKey   string `json:"secretKey"`
	SecretToken string `json:"secretToken"`
}

// InitMinFSConfig - Initialize MinFS configuration file.
func InitMinFSConfig() (*AccessConfig, error) {
	// Create db directory.
	if err := os.MkdirAll(globalDBDir, 0777); err != nil {
		return nil, err
	}
	// Config doesn't exist create it based on environment values.
	if _, err := os.Stat(globalConfigFile); err != nil {
		if os.IsNotExist(err) {
			log.Println("Initializing config.json for the first time, please update your access credentials.")
			ac := &AccessConfig{
				Version:     "1",
				AccessKey:   os.Getenv("MINFS_ACCESS_KEY"),
				SecretKey:   os.Getenv("MINFS_SECRET_KEY"),
				SecretToken: os.Getenv("MINFS_SECRET_TOKEN"),
			}
			acBytes, jerr := json.Marshal(ac)
			if jerr != nil {
				return nil, jerr
			}
			if err = os.WriteFile(globalConfigFile, acBytes, 0600); err != nil {
				return nil, err
			}
			return ac, nil
		} // Exists but not accessible, fail.
		return nil, err
	} // Config exists, proceed to read.
	acBytes, err := os.ReadFile(globalConfigFile)
	if err != nil {
		return nil, err
	}
	ac := &AccessConfig{}
	if err = json.Unmarshal(acBytes, ac); err != nil {
		return nil, err
	}
	// Override if access keys are set through env.
	accessKey := os.Getenv("MINFS_ACCESS_KEY")
	secretKey := os.Getenv("MINFS_SECRET_KEY")
	secretToken := os.Getenv("MINFS_SECRET_TOKEN")
	if accessKey != "" {
		ac.AccessKey = accessKey
	}
	if secretKey != "" {
		ac.SecretKey = secretKey
	}
	if secretToken != "" {
		ac.SecretToken = secretToken
	}
	return ac, nil
}

// Mountpoint configures the target mountpoint
func Mountpoint(mountpoint string) func(*Config) {
	return func(cfg *Config) {
		cfg.mountpoint = mountpoint
	}
}

// Target url target option for Config
func Target(target string) func(*Config) {
	return func(cfg *Config) {
		if u, err := url.Parse(target); err == nil {
			cfg.target = u

			if len(u.Path) > 1 {
				parts := strings.Split(u.Path[1:], "/")
				if len(parts) >= 0 {
					cfg.bucket = parts[0]
				}
				if len(parts) >= 1 {
					cfg.basePath = path.Join(parts[1:]...)
				}
			}
		}
	}
}

// CacheDir - cache directory path option for Config
func CacheDir(path string) func(*Config) {
	return func(cfg *Config) {
		cfg.cache = path
	}
}

// SetGID - sets a custom gid for the mount.
func SetGID(gid uint32) func(*Config) {
	return func(cfg *Config) {
		cfg.gid = gid
	}
}

// SetUID - sets a custom uid for the mount.
func SetUID(uid uint32) func(*Config) {
	return func(cfg *Config) {
		cfg.uid = uid
	}
}

// Insecure - enable insecure mode.
func Insecure() func(*Config) {
	return func(cfg *Config) {
		cfg.insecure = true
	}
}

// Debug - enables debug logging.
func Debug() func(*Config) {
	return func(cfg *Config) {
		cfg.debug = true
	}
}

// ScanTTL sets how long a directory scan result is cached before re-fetching
// from S3. A value of 0 disables TTL (scan once, never refresh).
func ScanTTL(ttl time.Duration) func(*Config) {
	return func(cfg *Config) {
		cfg.scanTTL = ttl
	}
}

// MetricsAddr sets the listen address for the metrics and health HTTP server.
// Pass an empty string to disable the server.
func MetricsAddr(addr string) func(*Config) {
	return func(cfg *Config) {
		cfg.metricsAddr = addr
	}
}

// ReadOnly disables all write operations on the mounted filesystem.
func ReadOnly() func(*Config) {
	return func(cfg *Config) {
		cfg.readOnly = true
	}
}

// CacheQuota sets the maximum total size in bytes for the local cache directory.
// A value of 0 means unlimited.
func CacheQuota(bytes int64) func(*Config) {
	return func(cfg *Config) {
		cfg.cacheQuota = bytes
	}
}

// Validates the config for sane values.
func (cfg *Config) validate() error {
	// check if mountpoint exists
	if cfg.mountpoint == "" {
		return errors.New("Mountpoint not set")
	}

	if cfg.target == nil {
		return errors.New("Target not set")
	}

	if cfg.bucket == "" {
		return errors.New("Bucket not set")
	}

	// Verify mountpoint directory exists.
	info, err := os.Stat(cfg.mountpoint)
	if err != nil {
		return fmt.Errorf("mountpoint %s: %w", cfg.mountpoint, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("mountpoint %s is not a directory", cfg.mountpoint)
	}

	// Check if mountpoint is already in use (prevent double-mount).
	if isMounted(cfg.mountpoint) {
		return fmt.Errorf("mountpoint %s is already mounted", cfg.mountpoint)
	}

	return nil
}

// isMounted checks /proc/mounts to determine if a path is already a mount point.
func isMounted(mountpoint string) bool {
	f, err := os.Open("/proc/mounts")
	if err != nil {
		return false // Cannot check; let FUSE mount fail naturally.
	}
	defer f.Close()

	// Resolve to absolute path for comparison.
	absMount, err := filepath.Abs(mountpoint)
	if err != nil {
		absMount = mountpoint
	}

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) >= 2 {
			if fields[1] == absMount {
				return true
			}
		}
	}
	return false
}

// --- Refreshable credentials provider ---

// refreshableProvider is a minio credentials.Provider that re-reads the
// access keys from the config file and environment on every Retrieve() call.
// This supports STS token rotation without restarting MinFS.
type refreshableProvider struct {
	configFile string
	fallbackAK string
	fallbackSK string
	fallbackST string
}

func (p *refreshableProvider) Retrieve() (credentials.Value, error) {
	ak, sk, st := p.fallbackAK, p.fallbackSK, p.fallbackST

	// Re-read config file on every call to pick up rotated credentials.
	if data, err := os.ReadFile(p.configFile); err == nil {
		var ac AccessConfig
		if json.Unmarshal(data, &ac) == nil {
			if ac.AccessKey != "" {
				ak = ac.AccessKey
			}
			if ac.SecretKey != "" {
				sk = ac.SecretKey
			}
			if ac.SecretToken != "" {
				st = ac.SecretToken
			}
		}
	}

	// Environment variables override file-based credentials.
	if v := os.Getenv("MINFS_ACCESS_KEY"); v != "" {
		ak = v
	}
	if v := os.Getenv("MINFS_SECRET_KEY"); v != "" {
		sk = v
	}
	if v := os.Getenv("MINFS_SECRET_TOKEN"); v != "" {
		st = v
	}

	return credentials.Value{
		AccessKeyID:     ak,
		SecretAccessKey: sk,
		SessionToken:    st,
		SignerType:      credentials.SignatureV4,
	}, nil
}

func (p *refreshableProvider) IsExpired() bool {
	// Always re-read — minio-go will cache the Value until IsExpired returns true.
	return true
}

// newRefreshableCreds creates a credentials.Credentials that re-reads the
// config file and environment on every S3 request, enabling STS token
// rotation without restart. Falls back to the initially configured keys.
func newRefreshableCreds(accessKey, secretKey, token, configFile string) *credentials.Credentials {
	p := &refreshableProvider{
		configFile: configFile,
		fallbackAK: accessKey,
		fallbackSK: secretKey,
		fallbackST: token,
	}
	return credentials.New(p)
}
