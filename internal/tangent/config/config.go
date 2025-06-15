package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/BurntSushi/toml"
)

// StdioRunnerConfig holds stdio runner related configuration
type StdioRunnerConfig struct {
	ScriptDir string `toml:"script_dir"` // Directory containing scripts
}

// AuthConfig holds authentication-related configuration
type AuthConfig struct {
	TokenExpiry string `toml:"token_expiry"` // Token expiration time
}

// GetTokenExpiry returns the token expiry as time.Duration
func (a *AuthConfig) GetTokenExpiry() (time.Duration, error) {
	return ParseDuration(a.TokenExpiry)
}

// GetTokenExpiryOrDefault returns the token expiry as time.Duration
// or panics if the value is invalid
func (a *AuthConfig) GetTokenExpiryOrDefault() time.Duration {
	duration, err := a.GetTokenExpiry()
	if err != nil {
		panic(fmt.Sprintf("invalid token expiry: %v", err))
	}
	return duration
}

// TansiveServerConfig holds tansive server related configuration
type TansiveServerConfig struct {
	HostName string `toml:"host_name"` // Tansive server hostname
	Port     string `toml:"port"`      // Tansive server port
}

func (t *TansiveServerConfig) GetURL() string {
	return "http://" + t.HostName + ":" + t.Port
}

// ConfigParam holds all configuration parameters for the tangent service
type ConfigParam struct {
	// Configuration version
	FormatVersion string `toml:"format_version"` // Version of this configuration file format

	// Server configuration
	ServerHostName string `toml:"server_hostname"` // Hostname for the server
	ServerPort     string `toml:"server_port"`     // Port for the server
	HandleCORS     bool   `toml:"handle_cors"`     // Whether to handle CORS

	// Stdio runner configuration
	StdioRunner StdioRunnerConfig `toml:"stdio_runner"`

	// Auth configuration
	Auth AuthConfig `toml:"auth"`

	// Tansive server configuration
	TansiveServer TansiveServerConfig `toml:"tansive_server"`
}

var cfg *ConfigParam

// Config returns the current configuration
func Config() *ConfigParam {
	return cfg
}

// ParseDuration parses a duration string in the format "<number><unit>" where unit can be:
// - y: years
// - d: days
// - h: hours
// - m: minutes
func ParseDuration(input string) (time.Duration, error) {
	if len(input) < 2 {
		return 0, fmt.Errorf("invalid input format")
	}

	// Extract the unit and the value from the input string
	unit := input[len(input)-1:]
	valueStr := input[:len(input)-1]
	value, err := strconv.Atoi(valueStr)
	if err != nil {
		return 0, fmt.Errorf("invalid number: %s", err)
	}

	// Convert the value to a duration based on the unit
	var duration time.Duration
	switch unit {
	case "d":
		duration = time.Duration(value) * 24 * time.Hour
	case "h":
		duration = time.Duration(value) * time.Hour
	case "m":
		duration = time.Duration(value) * time.Minute
	case "y":
		// Assuming 1 year = 365 days for simplicity
		duration = time.Duration(value) * 365 * 24 * time.Hour
	default:
		return 0, fmt.Errorf("unknown time unit: %s", unit)
	}

	return duration, nil
}

// ValidateConfig checks if all required configuration values are present and valid
func ValidateConfig(cfg *ConfigParam) error {
	// Check if the config file format version is supported
	if cfg.FormatVersion != ConfigFormatVersion {
		return fmt.Errorf("unsupported config file format version: %s", cfg.FormatVersion)
	}

	// Server validation
	if cfg.ServerPort == "" {
		return fmt.Errorf("server_port is required")
	}

	// Auth validation
	if cfg.Auth.TokenExpiry == "" {
		return fmt.Errorf("auth.token_expiry is required")
	}
	if _, err := ParseDuration(cfg.Auth.TokenExpiry); err != nil {
		return fmt.Errorf("invalid auth.token_expiry: %v", err)
	}

	// Tansive server validation
	if cfg.TansiveServer.HostName == "" {
		return fmt.Errorf("tansive_server.host_name is required")
	}
	if cfg.TansiveServer.Port == "" {
		return fmt.Errorf("tansive_server.port is required")
	}

	return nil
}

// LoadConfig loads configuration from a file
func LoadConfig(filename string) error {
	if filename == "" {
		return fmt.Errorf("config filename is required")
	}

	// Read and parse the config file
	content, err := os.ReadFile(filename)
	if err != nil {
		return fmt.Errorf("error reading config file: %v", err)
	}

	cfg = &ConfigParam{}
	if _, err := toml.Decode(string(content), cfg); err != nil {
		return fmt.Errorf("error parsing config file: %v", err)
	}

	// Validate the configuration
	if err := ValidateConfig(cfg); err != nil {
		return fmt.Errorf("invalid configuration: %v", err)
	}

	return nil
}

func GetAuditLogDir() string {
	// get os application data directory
	appDataDir, err := os.UserConfigDir()
	if err != nil {
		panic(err)
	}
	auditLogDir := filepath.Join(appDataDir, "tangent", "auditlogs")
	return auditLogDir
}

func CreateAuditLogDir() {
	auditLogDir := GetAuditLogDir()
	if _, err := os.Stat(auditLogDir); os.IsNotExist(err) {
		os.MkdirAll(auditLogDir, 0755)
	}
}

// ConfigFormatVersion is the current version of the configuration file format
const ConfigFormatVersion = "0.1.0"

func init() {
	if err := LoadConfig("tangent.conf"); err != nil {
		// Log the error but don't panic
		fmt.Printf("Error loading config: %v\n", err)
	}
	CreateAuditLogDir()
}

func TestInit() {
	wd, err := os.Getwd()
	if err != nil {
		panic(err)
	}

	// Check if we're already in the project root by looking for go.mod
	projectRoot := wd
	for {
		if _, err := os.Stat(filepath.Join(projectRoot, "go.mod")); err == nil {
			break
		}
		parent := filepath.Dir(projectRoot)
		if parent == projectRoot {
			panic("could not find project root (go.mod)")
		}
		projectRoot = parent
	}
	if err := LoadConfig(filepath.Join(projectRoot, "tangent.conf")); err != nil {
		panic(fmt.Errorf("error loading config: %v", err))
	}
	CreateAuditLogDir()
}
