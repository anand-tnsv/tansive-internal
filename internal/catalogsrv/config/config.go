package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/BurntSushi/toml"
)

// SessionConfig holds session-related configuration
type SessionConfig struct {
	ExpirationTime string `toml:"expiration_time"` // Default session expiration time
	MaxVariables   int    `toml:"max_variables"`   // Maximum number of variables allowed in a session
}

// GetExpirationTime returns the session expiration time as time.Duration
func (s *SessionConfig) GetExpirationTime() (time.Duration, error) {
	return ParseDuration(s.ExpirationTime)
}

// GetExpirationTimeOrDefault returns the session expiration time as time.Duration
// or panics if the value is invalid
func (s *SessionConfig) GetExpirationTimeOrDefault() time.Duration {
	duration, err := s.GetExpirationTime()
	if err != nil {
		panic(fmt.Sprintf("invalid session expiration time: %v", err))
	}
	return duration
}

// AuthConfig holds authentication-related configuration
type AuthConfig struct {
	MaxTokenAge          string `toml:"max_token_age"`          // Maximum age for tokens
	ClockSkew            string `toml:"clock_skew"`             // Allowed clock skew for time-based claims
	KeyEncryptionPasswd  string `toml:"key_encryption_passwd"`  // Password for key encryption
	DefaultTokenValidity string `toml:"default_token_validity"` // Default token validity duration
	FakeSingleUserToken  string `toml:"fake_single_user_token"` // Token for single user mode
}

// GetMaxTokenAge returns the maximum token age as time.Duration
func (a *AuthConfig) GetMaxTokenAge() (time.Duration, error) {
	return ParseDuration(a.MaxTokenAge)
}

// GetClockSkew returns the allowed clock skew as time.Duration
func (a *AuthConfig) GetClockSkew() (time.Duration, error) {
	return ParseDuration(a.ClockSkew)
}

// GetDefaultTokenValidity returns the default token validity as time.Duration
func (a *AuthConfig) GetDefaultTokenValidity() (time.Duration, error) {
	return ParseDuration(a.DefaultTokenValidity)
}

// GetMaxTokenAgeOrDefault returns the maximum token age as time.Duration
// or panics if the value is invalid
func (a *AuthConfig) GetMaxTokenAgeOrDefault() time.Duration {
	duration, err := a.GetMaxTokenAge()
	if err != nil {
		panic(fmt.Sprintf("invalid max token age: %v", err))
	}
	return duration
}

// GetClockSkewOrDefault returns the allowed clock skew as time.Duration
// or panics if the value is invalid
func (a *AuthConfig) GetClockSkewOrDefault() time.Duration {
	duration, err := a.GetClockSkew()
	if err != nil {
		panic(fmt.Sprintf("invalid clock skew: %v", err))
	}
	return duration
}

// GetDefaultTokenValidityOrDefault returns the default token validity as time.Duration
// or panics if the value is invalid
func (a *AuthConfig) GetDefaultTokenValidityOrDefault() time.Duration {
	duration, err := a.GetDefaultTokenValidity()
	if err != nil {
		panic(fmt.Sprintf("invalid default token validity: %v", err))
	}
	return duration
}

// ConfigParam holds all configuration parameters for the catalog service
type ConfigParam struct {
	// Configuration version
	FormatVersion string `toml:"format_version"` // Version of this configuration file format

	// Server configuration
	ServerHostName     string `toml:"server_hostname"`       // Hostname for the server
	ServerPort         string `toml:"server_port"`           // Port for the main server
	EndpointPort       string `toml:"endpoint_port"`         // Port for the endpoint server
	HandleCORS         bool   `toml:"handle_cors"`           // Whether to handle CORS
	MaxRequestBodySize int64  `toml:"max_request_body_size"` // Maximum size of request body in bytes

	// Session configuration
	Session SessionConfig `toml:"session"`

	// Auth configuration
	Auth AuthConfig `toml:"auth"`

	// Single user mode configuration
	SingleUserMode   bool   `toml:"single_user_mode"`   // Whether to run in single user mode
	DefaultTenantID  string `toml:"default_tenant_id"`  // Default tenant ID for single user mode
	DefaultProjectID string `toml:"default_project_id"` // Default project ID for single user mode

	// Database configuration
	DB struct {
		Host     string `toml:"host"`     // Database host
		Port     int    `toml:"port"`     // Database port
		DBName   string `toml:"dbname"`   // Database name
		User     string `toml:"user"`     // Database user
		Password string `toml:"password"` // Database password
		SSLMode  string `toml:"sslmode"`  // SSL mode for database connection
	} `toml:"db"`
}

var cfg *ConfigParam

// Config returns the current configuration
func Config() *ConfigParam {
	return cfg
}

// DSN returns the database connection string
func (c *ConfigParam) DSN() string {
	return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		c.DB.Host, c.DB.Port, c.DB.User, c.DB.Password, c.DB.DBName, c.DB.SSLMode)
}

// HatchCatalogDSN returns the DSN for the Hatch Catalog database
func HatchCatalogDSN() string {
	return cfg.DSN()
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
	if cfg.EndpointPort == "" {
		return fmt.Errorf("endpoint_port is required")
	}

	// Session validation
	if cfg.Session.ExpirationTime == "" {
		return fmt.Errorf("session.expiration_time is required")
	}
	if _, err := ParseDuration(cfg.Session.ExpirationTime); err != nil {
		return fmt.Errorf("invalid session.expiration_time: %v", err)
	}
	if cfg.Session.MaxVariables <= 0 {
		return fmt.Errorf("session.max_variables must be positive")
	}

	// Auth validation
	if cfg.Auth.MaxTokenAge == "" {
		return fmt.Errorf("auth.max_token_age is required")
	}
	if _, err := ParseDuration(cfg.Auth.MaxTokenAge); err != nil {
		return fmt.Errorf("invalid auth.max_token_age: %v", err)
	}
	if cfg.Auth.ClockSkew == "" {
		return fmt.Errorf("auth.clock_skew is required")
	}
	if _, err := ParseDuration(cfg.Auth.ClockSkew); err != nil {
		return fmt.Errorf("invalid auth.clock_skew: %v", err)
	}
	if cfg.Auth.DefaultTokenValidity == "" {
		return fmt.Errorf("auth.default_token_validity is required")
	}
	if _, err := ParseDuration(cfg.Auth.DefaultTokenValidity); err != nil {
		return fmt.Errorf("invalid auth.default_token_validity: %v", err)
	}

	// Single user mode validation
	if cfg.SingleUserMode {
		if cfg.DefaultTenantID == "" {
			return fmt.Errorf("default_tenant_id is required in single user mode")
		}
		if cfg.DefaultProjectID == "" {
			return fmt.Errorf("default_project_id is required in single user mode")
		}
		if cfg.Auth.FakeSingleUserToken == "" {
			return fmt.Errorf("auth.fake_single_user_token is required in single user mode")
		}
	}

	// Database validation
	if cfg.DB.Host == "" {
		return fmt.Errorf("db.host is required")
	}
	if cfg.DB.Port <= 0 {
		return fmt.Errorf("db.port must be positive")
	}
	if cfg.DB.DBName == "" {
		return fmt.Errorf("db.dbname is required")
	}
	if cfg.DB.User == "" {
		return fmt.Errorf("db.user is required")
	}
	if cfg.DB.Password == "" {
		return fmt.Errorf("db.password is required")
	}
	if cfg.DB.SSLMode == "" {
		return fmt.Errorf("db.sslmode is required")
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

	// Generate key encryption password if not set
	if cfg.Auth.KeyEncryptionPasswd == "" {
		id := "catalogsrv.tansive.io"
		cfg.Auth.KeyEncryptionPasswd = id
	}

	return nil
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
	if err := LoadConfig(filepath.Join(projectRoot, "tansivesrv.conf")); err != nil {
		panic(fmt.Errorf("error loading config: %v", err))
	}
}
