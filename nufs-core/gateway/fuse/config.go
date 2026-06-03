//go:build linux

package fuse

import (
	"errors"
	"os"
	"path"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
)

type MountConfig struct {
	Mountpoint string
	MetaDir    string `json:"meta_dir,omitempty"`
	CacheDir   string `json:"cache_dir,omitempty"`

	MetricsAddr string        `json:"metrics_addr,omitempty"`
	ScanTTL     time.Duration `json:"scan_ttl,omitempty"`
	ReadOnly    bool          `json:"read_only,omitempty"`

	UID uint32 `json:"uid,omitempty"`
	GID uint32 `json:"gid,omitempty"`

	AllowOther bool `json:"allow_other,omitempty"`
	Debug      bool `json:"debug,omitempty"`
}

func DefaultMountConfig() *MountConfig {
	return &MountConfig{
		Mountpoint:  "/mnt/dfs",
		MetaDir:     "/var/lib/dfs/metadata",
		MetricsAddr: "",
		ScanTTL:     60 * time.Second,
		UID:         uint32(os.Getuid()),
		GID:         uint32(os.Getgid()),
		AllowOther:  false,
	}
}

func (c *MountConfig) Validate() error {
	if c.Mountpoint == "" {
		return errors.New("mountpoint not set")
	}
	if c.MetaDir == "" {
		return errors.New("meta dir not set")
	}
	if c.ScanTTL <= 0 {
		c.ScanTTL = 60 * time.Second
	}
	return nil
}

func (c *MountConfig) FUSEOptions() *fuse.MountOptions {
	return &fuse.MountOptions{
		Name:       "dfs",
		FsName:     "dfs",
		AllowOther: c.AllowOther,
		Debug:      c.Debug,
	}
}

func (c *MountConfig) ResolveCacheDir() string {
	if c.CacheDir != "" {
		return c.CacheDir
	}
	return path.Join(c.MetaDir, "chunk-cache")
}
