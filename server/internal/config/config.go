// Package config handles loading and validating server configuration from
// YAML files and environment variable overrides.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

// PGConfig contains PostgreSQL wire protocol listener settings.
type PGConfig struct {
	Enabled  bool              `yaml:"enabled"`
	Host     string            `yaml:"host"`
	Port     int               `yaml:"port"`
	AuthMode string            `yaml:"auth_mode"` // "trust", "cleartext"
	Users    map[string]string `yaml:"users"`
	MaxConns int               `yaml:"max_conns"`
}

// Config holds all runtime configuration for the OxenDB server.
type Config struct {
	Server   ServerConfig   `yaml:"server"`
	Database DatabaseConfig `yaml:"database"`
	Auth     AuthConfig     `yaml:"auth"`
	Metrics  MetricsConfig  `yaml:"metrics"`
	Log      LogConfig      `yaml:"log"`
	PG       PGConfig       `yaml:"pg"`
}

// ServerConfig contains HTTP server settings.
type ServerConfig struct {
	Host            string `yaml:"host"`
	Port            int    `yaml:"port"`
	ReadTimeoutSec  int    `yaml:"read_timeout_sec"`
	WriteTimeoutSec int    `yaml:"write_timeout_sec"`
	MaxConns        int    `yaml:"max_conns"`
}

// DatabaseConfig points to the Rust storage engine data directory.
type DatabaseConfig struct {
	DataDir            string `yaml:"data_dir"`
	MemtableSizeMB     int    `yaml:"memtable_size_mb"`
	BlockSizeKB        int    `yaml:"block_size_kb"`
	BloomBitsPerKey    int    `yaml:"bloom_bits_per_key"`
	BlockCacheMB       int    `yaml:"block_cache_mb"`
	L0CompactionTrigger int   `yaml:"l0_compaction_trigger"`
}

// AuthConfig controls bearer-token authentication.
type AuthConfig struct {
	Enabled bool   `yaml:"enabled"`
	Token   string `yaml:"token"` // can also be set via OXEN_AUTH_TOKEN env var
}

// MetricsConfig controls Prometheus metrics exposure.
type MetricsConfig struct {
	Enabled bool   `yaml:"enabled"`
	Path    string `yaml:"path"`
}

// LogConfig sets the logging level and format.
type LogConfig struct {
	Level  string `yaml:"level"`  // debug, info, warn, error
	Format string `yaml:"format"` // text, json
}

// Default returns a Config populated with safe defaults.
func Default() *Config {
	return &Config{
		Server: ServerConfig{
			Host:            "0.0.0.0",
			Port:            8080,
			ReadTimeoutSec:  30,
			WriteTimeoutSec: 30,
			MaxConns:        1000,
		},
		Database: DatabaseConfig{
			DataDir:             "./oxendb_data",
			MemtableSizeMB:      64,
			BlockSizeKB:         4,
			BloomBitsPerKey:     10,
			BlockCacheMB:        128,
			L0CompactionTrigger: 4,
		},
		Auth: AuthConfig{
			Enabled: false,
			Token:   "",
		},
		Metrics: MetricsConfig{
			Enabled: true,
			Path:    "/metrics",
		},
		Log: LogConfig{
			Level:  "info",
			Format: "text",
		},
		PG: PGConfig{
			Enabled:  true,
			Host:     "0.0.0.0",
			Port:     5432,
			AuthMode: "trust",
			MaxConns: 100,
		},
	}
}

// PGAddr returns the combined host:port listen address for the PostgreSQL wire protocol.
func (c *Config) PGAddr() string {
	return fmt.Sprintf("%s:%d", c.PG.Host, c.PG.Port)
}

// Load reads configuration from a YAML file and applies environment variable
// overrides. If path is empty, only the defaults and env vars are used.
func Load(path string) (*Config, error) {
	cfg := Default()

	if path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("config: read file %q: %w", path, err)
		}
		if err := yaml.Unmarshal(data, cfg); err != nil {
			return nil, fmt.Errorf("config: parse YAML %q: %w", path, err)
		}
	}

	applyEnv(cfg)

	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

// Addr returns the combined host:port listen address.
func (c *Config) Addr() string {
	return fmt.Sprintf("%s:%d", c.Server.Host, c.Server.Port)
}

// Validate checks that all required fields are present and valid.
func (c *Config) Validate() error {
	if c.Server.Port < 1 || c.Server.Port > 65535 {
		return fmt.Errorf("config: server.port %d out of range", c.Server.Port)
	}
	if c.Database.DataDir == "" {
		return fmt.Errorf("config: database.data_dir must not be empty")
	}
	if c.Auth.Enabled && c.Auth.Token == "" {
		return fmt.Errorf("config: auth.enabled is true but auth.token is empty")
	}
	return nil
}

// applyEnv overrides config fields from well-known environment variables.
func applyEnv(cfg *Config) {
	if v := os.Getenv("OXEN_HOST"); v != "" {
		cfg.Server.Host = v
	}
	if v := os.Getenv("OXEN_PORT"); v != "" {
		if p, err := strconv.Atoi(v); err == nil {
			cfg.Server.Port = p
		}
	}
	if v := os.Getenv("OXEN_DATA_DIR"); v != "" {
		cfg.Database.DataDir = v
	}
	if v := os.Getenv("OXEN_AUTH_TOKEN"); v != "" {
		cfg.Auth.Token = v
		cfg.Auth.Enabled = true
	}
	if v := os.Getenv("OXEN_LOG_LEVEL"); v != "" {
		cfg.Log.Level = strings.ToLower(v)
	}
	if v := os.Getenv("OXEN_MAX_CONNS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.Server.MaxConns = n
		}
	}
	if v := os.Getenv("OXEN_PG_PORT"); v != "" {
		if p, err := strconv.Atoi(v); err == nil {
			cfg.PG.Port = p
		}
	}
	if v := os.Getenv("OXEN_PG_AUTH_MODE"); v != "" {
		cfg.PG.AuthMode = strings.ToLower(v)
	}
}
