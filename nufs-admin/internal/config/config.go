// Package config provides configuration loading and hot-reload support.
package config

import (
	"os"
	"os/signal"
	"sync"
	"syscall"

	"gopkg.in/yaml.v3"
)

// ClusterConfig represents a single NUFS cluster configuration.
type ClusterConfig struct {
	Name        string `yaml:"name"`          // Cluster identifier, e.g., "bj-prod"
	Region      string `yaml:"region"`        // Geographic region, e.g., "beijing"
	MetadOpsURL string `yaml:"metad_ops_url"` // metad ops API endpoint, e.g., "http://10.0.1.3:8091"
	MetadToken  string `yaml:"metad_token"`   // Optional Bearer token for metad ops API (production auth, AUTH_TOKEN)
	Description string `yaml:"description"`   // Human-readable description
}

// ServerConfig holds admin-server specific settings.
type ServerConfig struct {
	Listen    string `yaml:"listen"`     // HTTP listen address, e.g., ":8090"
	JWTSecret string `yaml:"jwt_secret"` // JWT signing secret
}

// AuthConfig holds authentication settings.
type AuthConfig struct {
	UsersFile string `yaml:"users_file"` // Path to users.yaml
}

// DatabaseConfig holds MySQL connection settings.
type DatabaseConfig struct {
	DSN string `yaml:"dsn"` // MySQL DSN, e.g., "user:pass@tcp(127.0.0.1:3306)/nufs_admin?parseTime=true"
}

// Config is the top-level configuration structure.
type Config struct {
	Clusters []ClusterConfig `yaml:"clusters"`
	Server   ServerConfig    `yaml:"server"`
	Auth     AuthConfig      `yaml:"auth"`
	Database DatabaseConfig  `yaml:"database"`
}

// Manager handles configuration loading and hot-reload.
type Manager struct {
	path     string
	current  *Config
	mu       sync.RWMutex
	onChange func(*Config)
}

// Load reads configuration from a YAML file.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}

	return &cfg, nil
}

// NewManager creates a configuration manager with hot-reload support.
func NewManager(path string, onChange func(*Config)) (*Manager, error) {
	cfg, err := Load(path)
	if err != nil {
		return nil, err
	}

	m := &Manager{
		path:     path,
		current:  cfg,
		onChange: onChange,
	}

	// Setup SIGHUP handler for hot-reload
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGHUP)

	go func() {
		for range sigCh {
			newCfg, err := Load(m.path)
			if err != nil {
				// Log error but keep current config
				continue
			}

			m.mu.Lock()
			m.current = newCfg
			m.mu.Unlock()

			if m.onChange != nil {
				m.onChange(newCfg)
			}
		}
	}()

	return m, nil
}

// Get returns the current configuration (thread-safe).
func (m *Manager) Get() *Config {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.current
}