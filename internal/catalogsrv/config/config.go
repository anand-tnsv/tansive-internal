package config

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/BurntSushi/toml"
)

type ConfigParam struct {
	ServerPort               string `toml:"server_port"`
	EndpointPort             string `toml:"endpoint_port"`
	HandleCORS               bool   `toml:"handle_cors"`
	ClientConfig             string `toml:"client_config"`
	InternalCA               string `toml:"internal_ca"`
	InternalServerCert       string `toml:"internal_server_cert"`
	InternalServerPrivateKey string `toml:"internal_server_private_key"`
	IDTokenValidity          int    `toml:"id_token_validity"`
	APITokenValidity         string `toml:"api_token_validity"`
}

var cfg *ConfigParam

func Config() *ConfigParam {
	return cfg
}

//TODO - Check all config value usage and assign defaults

func LoadConfig(filename string) error {
	if filename == "" {
		cfg = &ConfigParam{
			ServerPort:   "8194",
			EndpointPort: "9002",
			HandleCORS:   true,
		}
		return nil
	}
	// Read the config file
	content, err := os.ReadFile(filename)
	if err != nil {
		return fmt.Errorf("error reading config file: %v", err)
	}
	// Parse the config file
	var cp ConfigParam
	if _, err := toml.Decode(string(content), &cp); err != nil {
		return fmt.Errorf("error parsing config file: %v", err)
	}
	// assign config to global cfg
	cfg = &cp
	return nil
}

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
	err := LoadConfig("")
	if err != nil {
		panic(err)
	}
}
