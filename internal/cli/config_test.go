package cli

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestConfig(t *testing.T) {
	// Create a temporary directory for test files
	tmpDir, err := os.MkdirTemp("", "config_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Test cases
	tests := []struct {
		name    string
		config  string
		wantErr bool
	}{
		{
			name: "valid config",
			config: `version: 1.0
server:port: "example.com:8080"
api_key: "test-key"
roles:
  - name: "admin"
    access_token: "token1"
    valid_before: "2024-12-31"`,
			wantErr: false,
		},
		{
			name: "missing server port",
			config: `version: 1.0
api_key: "test-key"
roles:
  - name: "admin"
    access_token: "token1"
    valid_before: "2024-12-31"`,
			wantErr: true,
		},
		{
			name: "missing api key",
			config: `version: 1.0
server:port: "example.com:8080"
roles:
  - name: "admin"
    access_token: "token1"
    valid_before: "2024-12-31"`,
			wantErr: true,
		},
		{
			name: "invalid server port format - missing port",
			config: `version: 1.0
server:port: "example.com"
api_key: "test-key"
roles:
  - name: "admin"
    access_token: "token1"
    valid_before: "2024-12-31"`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a temporary config file
			configFile := filepath.Join(tmpDir, "config.yaml")
			if err := os.WriteFile(configFile, []byte(tt.config), 0644); err != nil {
				t.Fatalf("Failed to write test config: %v", err)
			}

			// Test LoadConfig
			err := LoadConfig(configFile)
			if (err != nil) != tt.wantErr {
				t.Errorf("LoadConfig() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr {
				cfg := GetConfig()
				if cfg == nil {
					t.Error("GetConfig() returned nil")
					return
				}

				// Test ValidateConfig
				if err := cfg.ValidateConfig(); err != nil {
					t.Errorf("ValidateConfig() error = %v", err)
				}

				// Test GetServerURL
				serverURL := cfg.GetServerURL()
				if serverURL == "" {
					t.Error("GetServerURL() returned empty string")
				}
				if !strings.HasPrefix(serverURL, "http://") {
					t.Errorf("GetServerURL() = %v, want prefix http://", serverURL)
				}
			}
		})
	}
}

func TestMorphServer(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "empty string",
			input:    "",
			expected: "",
		},
		{
			name:     "no protocol",
			input:    "example.com:8080",
			expected: "http://example.com:8080",
		},
		{
			name:     "with http",
			input:    "http://example.com:8080",
			expected: "http://example.com:8080",
		},
		{
			name:     "with https",
			input:    "https://example.com:8080",
			expected: "https://example.com:8080",
		},
		{
			name:     "with trailing slash",
			input:    "http://example.com:8080/",
			expected: "http://example.com:8080",
		},
		{
			name:     "with multiple trailing slashes",
			input:    "http://example.com:8080///",
			expected: "http://example.com:8080",
		},
		{
			name:     "with port but no protocol",
			input:    "local.tansive.dev:8080",
			expected: "http://local.tansive.dev:8080",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := MorphServer(tt.input)
			if got != tt.expected {
				t.Errorf("MorphServer() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestWriteConfig(t *testing.T) {
	// Create a temporary directory for test files
	tmpDir, err := os.MkdirTemp("", "config_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cfg := &Config{
		Version:    "1.0",
		ServerPort: "example.com:8080",
		APIKey:     "test-key",
	}

	// Test WriteConfig
	configFile := filepath.Join(tmpDir, "config.yaml")
	err = cfg.WriteConfig(configFile)
	if err != nil {
		t.Errorf("WriteConfig() error = %v", err)
	}

	// Verify the file was created
	if _, err := os.Stat(configFile); os.IsNotExist(err) {
		t.Error("WriteConfig() did not create the file")
	}

	// Test writing to invalid path
	err = cfg.WriteConfig("")
	if err == nil {
		t.Error("WriteConfig() should return error for empty file path")
	}
}
