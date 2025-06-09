package config

import (
	"fmt"
	"os"
	"time"

	"github.com/BurntSushi/toml"
)

// ConfigParam holds all configuration parameters for the tangent service
type ConfigParam struct {
	// Server configuration
	ServerHostName string `toml:"server_hostname"` // Hostname for the server
	ServerPort     string `toml:"server_port"`     // Port for the server
	HandleCORS     bool   `toml:"handle_cors"`     // Whether to handle CORS

	StdioRunner struct {
		ScriptDir string `toml:"script_dir"`
	} `toml:"stdio_runner"`

	Auth struct {
		TokenExpiry time.Duration `toml:"token_expiry"`
	} `toml:"auth"`
}

var cfg *ConfigParam

// Config returns the current configuration
func Config() *ConfigParam {
	return cfg
}

// LoadConfig loads configuration from a file or sets defaults if no file is provided
func LoadConfig(filename string) error {
	// Initialize with default values
	cfg = &ConfigParam{
		ServerPort: "8194",
		HandleCORS: true,
	}

	if filename == "" {
		LoadDefaultsIfNotSet(cfg)
		return nil
	}

	// Read and parse the config file
	content, err := os.ReadFile(filename)
	if err != nil {
		return fmt.Errorf("error reading config file: %v", err)
	}

	if _, err := toml.Decode(string(content), cfg); err != nil {
		return fmt.Errorf("error parsing config file: %v", err)
	}

	LoadDefaultsIfNotSet(cfg)
	return nil
}

// LoadDefaultsIfNotSet sets default values for any unset configuration parameters
func LoadDefaultsIfNotSet(cfg *ConfigParam) {
	// Server defaults
	if cfg.ServerHostName == "" {
		cfg.ServerHostName = "localhost"
	}
	if cfg.ServerPort == "" {
		cfg.ServerPort = "8194"
	}
	cfg.HandleCORS = true
}

func init() {
	if err := LoadConfig(""); err != nil {
		// Log the error but don't panic
		fmt.Printf("Error loading config: %v\n", err)
	}
}
