package config

import (
	"fmt"
	"os"

	"github.com/BurntSushi/toml"
)

type ConfigParam struct {
	ServerPort   string `toml:"server_port"`
	HandleCORS   bool   `toml:"handle_cors"`
	ClientConfig string `toml:"client_config"`
}

var cfg *ConfigParam

func Config() *ConfigParam {
	return cfg
}

//TODO - Check all config value usage and assign defaults

func LoadConfig(filename string) error {
	if filename == "" {
		cfg = &ConfigParam{
			ServerPort: "8194",
			HandleCORS: true,
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

func init() {
	err := LoadConfig("")
	if err != nil {
		panic(err)
	}
}
