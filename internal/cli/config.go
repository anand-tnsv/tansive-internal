package cli

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

// DefaultConfigFile is the default name of the config file
const DefaultConfigFile = "config.yaml"

// Config represents the configuration for the Tansive CLI
// It contains server connection details and authentication information
type Config struct {
	// Version of the configuration file format
	Version string `yaml:"version"`
	// ServerPort is the URL and port of the Tansive server
	ServerPort string `yaml:"server:port"`
	// APIKey is the authentication token for the Tansive server
	APIKey string `yaml:"api_key"`
	// Roles contains the list of roles and their associated tokens
	Roles []Role `yaml:"roles"`
}

// Role represents a user role with associated access token
type Role struct {
	// Name of the role
	Name string `yaml:"name"`
	// AccessToken is the token associated with this role
	AccessToken string `yaml:"access_token"`
	// ValidBefore is the expiration date of the token
	ValidBefore string `yaml:"valid_before"`
}

var config *Config

// GetDefaultConfigPath returns the default path for the config file
// It uses the OS-specific config directory (e.g., ~/.config/tansive on Linux)
func GetDefaultConfigPath() (string, error) {
	configDir, err := os.UserConfigDir()
	if err != nil {
		return "", fmt.Errorf("failed to get user config directory: %w", err)
	}
	return filepath.Join(configDir, "tansive", DefaultConfigFile), nil
}

// LoadConfig loads the configuration from the specified file
// If no file is specified, it uses the default config location
func LoadConfig(file string) error {
	if file == "" {
		var err error
		file, err = GetDefaultConfigPath()
		if err != nil {
			return fmt.Errorf("failed to get default config path: %w", err)
		}
	}

	yamlStr, err := os.ReadFile(file)
	if err != nil {
		return fmt.Errorf("unable to read config file: %w", err)
	}

	var c Config
	if err = yaml.Unmarshal(yamlStr, &c); err != nil {
		return fmt.Errorf("unable to parse config file: %w", err)
	}

	// Validate required fields
	if c.ServerPort == "" {
		return errors.New("server:port is required")
	}
	if c.APIKey == "" {
		return errors.New("api_key is required")
	}

	// Validate server port format
	if !strings.Contains(c.ServerPort, ":") {
		return errors.New("server:port must include port number")
	}

	// Morph the server URL before storing
	c.ServerPort = MorphServer(c.ServerPort)

	config = &c
	return nil
}

// GetConfig returns the current configuration
func GetConfig() *Config {
	return config
}

// WriteConfig writes the current configuration to the specified file
// If no file is specified, it uses the default config location
func (cfg *Config) WriteConfig(file string) error {
	if file == "" {
		return errors.New("file path cannot be empty")
	}

	err := os.MkdirAll(filepath.Dir(file), os.ModePerm)
	if err != nil {
		return fmt.Errorf("unable to create config directory: %w", err)
	}

	yamlStr, err := yaml.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("unable to generate configuration: %w", err)
	}

	err = os.WriteFile(file, yamlStr, os.FileMode(0644))
	if err != nil {
		return fmt.Errorf("unable to write config file: %w", err)
	}

	return nil
}

// AddRole adds a new role to the configuration
// Returns an error if a role with the same name already exists
func (cfg *Config) AddRole(name, accessToken, validBefore string) error {
	// Check if role already exists
	for _, role := range cfg.Roles {
		if role.Name == name {
			return fmt.Errorf("role %s already exists", name)
		}
	}

	cfg.Roles = append(cfg.Roles, Role{
		Name:        name,
		AccessToken: accessToken,
		ValidBefore: validBefore,
	})

	return nil
}

// RemoveRole removes a role from the configuration
// Returns an error if the role does not exist
func (cfg *Config) RemoveRole(name string) error {
	for i, role := range cfg.Roles {
		if role.Name == name {
			cfg.Roles = append(cfg.Roles[:i], cfg.Roles[i+1:]...)
			return nil
		}
	}
	return fmt.Errorf("role %s not found", name)
}

// GetRole returns a role by name
// Returns an error if the role does not exist
func (cfg *Config) GetRole(name string) (*Role, error) {
	for _, role := range cfg.Roles {
		if role.Name == name {
			return &role, nil
		}
	}
	return nil, fmt.Errorf("role %s not found", name)
}

// ValidateConfig validates the configuration
// Checks for required fields and proper formatting
func (cfg *Config) ValidateConfig() error {
	if cfg.ServerPort == "" {
		return errors.New("server:port is required")
	}
	if !strings.HasPrefix(cfg.ServerPort, "http://") && !strings.HasPrefix(cfg.ServerPort, "https://") {
		return errors.New("server:port must start with http:// or https://")
	}
	if !strings.Contains(cfg.ServerPort, ":") {
		return errors.New("server:port must include port number")
	}
	if cfg.APIKey == "" {
		return errors.New("API key is required")
	}
	return nil
}

// Print prints the current configuration in a human-readable format
func (cfg *Config) Print() {
	fmt.Printf("Server: %s\n", cfg.ServerPort)
	fmt.Printf("API Key: %s\n", cfg.APIKey)
	fmt.Println("Roles:")
	for _, role := range cfg.Roles {
		fmt.Printf("  - %s (valid until: %s)\n", role.Name, role.ValidBefore)
	}
}

// MorphServer ensures the server URL is properly formatted
// Adds http:// prefix if missing and removes trailing slashes
func MorphServer(server string) string {
	if server == "" {
		return server
	}

	// Remove any trailing slashes
	server = strings.TrimRight(server, "/")

	// Add http:// if no protocol is specified
	if !strings.HasPrefix(server, "http://") && !strings.HasPrefix(server, "https://") {
		server = "http://" + server
	}

	return server
}

// GetServerURL returns the properly formatted server URL
func (cfg *Config) GetServerURL() string {
	return MorphServer(cfg.ServerPort)
}
