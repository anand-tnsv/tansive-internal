package config

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/denisbrodbeck/machineid"
)

type ConfigParam struct {
	ServerHostName       string `toml:"server_hostname"`
	ServerPort           string `toml:"server_port"`
	EndpointPort         string `toml:"endpoint_port"`
	HandleCORS           bool   `toml:"handle_cors"`
	KeyEncryptionPasswd  string `toml:"key_encryption_passwd"`
	DefaultTokenValidity string `toml:"default_token_validity"`
	SingleUserMode       bool   `toml:"single_user_mode"`
	DefaultTenantID      string `toml:"default_tenant_id"`
	DefaultProjectID     string `toml:"default_project_id"`
	FakeSingleUserToken  string `toml:"fake_single_user_token"`
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
		LoadDefaultsIfNotSet(cfg)
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

	LoadDefaultsIfNotSet(&cp)

	// assign config to global cfg
	cfg = &cp
	return nil
}

func LoadDefaultsIfNotSet(cfg *ConfigParam) {
	if cfg.ServerHostName == "" {
		cfg.ServerHostName = "localhost"
	}
	if cfg.ServerPort == "" {
		cfg.ServerPort = "8194"
	}
	if cfg.EndpointPort == "" {
		cfg.EndpointPort = "9002"
	}
	if cfg.DefaultTokenValidity == "" {
		cfg.DefaultTokenValidity = "3h"
	}
	if cfg.KeyEncryptionPasswd == "" {
		// Signing keys should be encrypted and managed using KMS. This is for local usage and should naver be used
		// in production. If the user set no password, we generate a reproducible password based on the machine id.
		id, err := machineid.ProtectedID("catalogsrv.tansive.io")
		if err != nil {
			panic("unable to obtain unique id to generate key passwd")
		}
		cfg.KeyEncryptionPasswd = id
	}
	cfg.SingleUserMode = true
	// Default tenant and project for single user
	cfg.DefaultTenantID = "TXYZABC"
	cfg.DefaultProjectID = "PXYZABC"
	cfg.FakeSingleUserToken = "single-user-fake-token"
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
