package server

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// Config is the server's YAML configuration, loaded via --config.
type Config struct {
	NodeID     string `yaml:"node_id"`
	Listen     string `yaml:"listen"`
	DataDir    string `yaml:"data_dir"`
	Fsync      string `yaml:"fsync"` // "always" or "never"
	JWTSecret  string `yaml:"jwt_secret"`
	AdminToken string `yaml:"admin_token"`
	RulesFile  string `yaml:"rules_file"`
	// DevMode enables the unauthenticated GET /dev/token?uid= endpoint for
	// demo tooling (PLAN.md M11). Never enable it on a network others can
	// reach: it mints valid tokens for any uid.
	DevMode bool `yaml:"dev_mode"`

	MaxOpsPerPush     int          `yaml:"max_ops_per_push"`
	SessionSendBuffer int          `yaml:"session_send_buffer"`
	Peers             []PeerConfig `yaml:"peers"` // used from M9
}

// PeerConfig describes one replication peer (used from M9).
type PeerConfig struct {
	ID     string `yaml:"id"`
	URL    string `yaml:"url"`
	Secret string `yaml:"secret"`
}

// FsyncAlways reports whether commits must be durable before acking.
func (c *Config) FsyncAlways() bool { return c.Fsync == "always" }

// LoadConfig reads and validates a server config file, applying defaults.
func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("load server config: %w", err)
	}
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse server config: %w", err)
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// validate checks required fields and applies defaults.
func (c *Config) validate() error {
	if c.NodeID == "" {
		return fmt.Errorf("server config: node_id is required")
	}
	if c.Listen == "" {
		return fmt.Errorf("server config: listen is required")
	}
	if c.DataDir == "" {
		return fmt.Errorf("server config: data_dir is required")
	}
	if c.JWTSecret == "" {
		return fmt.Errorf("server config: jwt_secret is required")
	}
	if c.RulesFile == "" {
		return fmt.Errorf("server config: rules_file is required")
	}
	switch c.Fsync {
	case "":
		c.Fsync = "always" // the safe default
	case "always", "never":
	default:
		return fmt.Errorf("server config: fsync must be \"always\" or \"never\", got %q", c.Fsync)
	}
	if c.MaxOpsPerPush == 0 {
		c.MaxOpsPerPush = 1000
	}
	if c.MaxOpsPerPush < 1 {
		return fmt.Errorf("server config: max_ops_per_push must be positive, got %d", c.MaxOpsPerPush)
	}
	if c.SessionSendBuffer == 0 {
		c.SessionSendBuffer = 1024
	}
	if c.SessionSendBuffer < 1 {
		return fmt.Errorf("server config: session_send_buffer must be positive, got %d", c.SessionSendBuffer)
	}
	seen := map[string]bool{}
	for i, peer := range c.Peers {
		if peer.ID == "" {
			return fmt.Errorf("server config: peers[%d].id is required", i)
		}
		if peer.URL == "" {
			return fmt.Errorf("server config: peers[%d].url is required", i)
		}
		if seen[peer.ID] {
			return fmt.Errorf("server config: duplicate peer id %q", peer.ID)
		}
		seen[peer.ID] = true
	}
	return nil
}
