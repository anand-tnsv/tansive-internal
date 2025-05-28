package config

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/denisbrodbeck/machineid"
)

// ConfigParam holds all configuration parameters for the catalog service
type ConfigParam struct {
	// Server configuration
	ServerHostName     string `toml:"server_hostname"`       // Hostname for the server
	ServerPort         string `toml:"server_port"`           // Port for the main server
	EndpointPort       string `toml:"endpoint_port"`         // Port for the endpoint server
	HandleCORS         bool   `toml:"handle_cors"`           // Whether to handle CORS
	MaxRequestBodySize int64  `toml:"max_request_body_size"` // Maximum size of request body in bytes

	// Auth configuration
	Auth struct {
		MaxTokenAge          time.Duration `toml:"max_token_age"`          // Maximum age for tokens
		ClockSkew            time.Duration `toml:"clock_skew"`             // Allowed clock skew for time-based claims
		KeyEncryptionPasswd  string        `toml:"key_encryption_passwd"`  // Password for key encryption
		DefaultTokenValidity string        `toml:"default_token_validity"` // Default token validity duration
		FakeSingleUserToken  string        `toml:"fake_single_user_token"` // Token for single user mode
	} `toml:"auth"`

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

// LoadConfig loads configuration from a file or sets defaults if no file is provided
func LoadConfig(filename string) error {
	// Initialize with default values
	cfg = &ConfigParam{
		ServerPort:     "8194",
		EndpointPort:   "9002",
		HandleCORS:     true,
		SingleUserMode: false, // Default to multi-user mode
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
	if cfg.EndpointPort == "" {
		cfg.EndpointPort = "9002"
	}
	if cfg.MaxRequestBodySize == 0 {
		cfg.MaxRequestBodySize = 1024
	}

	// Auth defaults
	if cfg.Auth.MaxTokenAge == 0 {
		cfg.Auth.MaxTokenAge = 24 * time.Hour
	}
	if cfg.Auth.ClockSkew == 0 {
		cfg.Auth.ClockSkew = 5 * time.Minute
	}
	if cfg.Auth.DefaultTokenValidity == "" {
		cfg.Auth.DefaultTokenValidity = "3h"
	}
	if cfg.Auth.KeyEncryptionPasswd == "" {
		// Signing keys should be encrypted and managed using KMS. This is for local usage and should never be used
		// in production. If the user set no password, we generate a reproducible password based on the machine id.
		id, err := machineid.ProtectedID("catalogsrv.tansive.io")
		if err != nil {
			panic("unable to obtain unique id to generate key passwd")
		}
		cfg.Auth.KeyEncryptionPasswd = id
	}

	cfg.SingleUserMode = true

	// Single user mode defaults
	if cfg.SingleUserMode {
		if cfg.DefaultTenantID == "" {
			cfg.DefaultTenantID = "TXYZABC"
		}
		if cfg.DefaultProjectID == "" {
			cfg.DefaultProjectID = "PXYZABC"
		}
		if cfg.Auth.FakeSingleUserToken == "" {
			cfg.Auth.FakeSingleUserToken = "single-user-fake-token"
		}
	}

	// Database defaults
	if cfg.DB.Host == "" {
		cfg.DB.Host = "localhost"
	}
	if cfg.DB.Port == 0 {
		cfg.DB.Port = 5432
	}
	if cfg.DB.User == "" {
		cfg.DB.User = "catalog_api"
	}
	if cfg.DB.Password == "" {
		cfg.DB.Password = "abc@123"
	}
	if cfg.DB.DBName == "" {
		cfg.DB.DBName = "hatchcatalog"
	}
	if cfg.DB.SSLMode == "" {
		cfg.DB.SSLMode = "disable"
	}
}

// ParseTokenDuration parses a duration string in the format "<number><unit>" where unit can be:
// - y: years
// - d: days
// - h: hours
// - m: minutes
func ParseTokenDuration(input string) (time.Duration, error) {
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

func init() {
	if err := LoadConfig(""); err != nil {
		// Log the error but don't panic
		fmt.Printf("Error loading config: %v\n", err)
	}
}
