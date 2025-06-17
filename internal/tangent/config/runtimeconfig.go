package config

import (
	"crypto/ed25519"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/avast/retry-go/v4"
	"github.com/rs/zerolog/log"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catcommon"
	srvtangent "github.com/tansive/tansive-internal/internal/catalogsrv/tangent"
	"github.com/tansive/tansive-internal/internal/common/httpclient"
	"github.com/tansive/tansive-internal/internal/common/uuid"
)

type KeyPair struct {
	PrivateKey []byte `json:"private_key"`
	PublicKey  []byte `json:"public_key"`
}

type RuntimeConfig struct {
	TangentID     uuid.UUID `json:"tangent_id"`
	Registered    bool      `json:"registered"`
	RegisteredAt  time.Time `json:"registered_at"`
	AccessKey     KeyPair   `json:"access_key"`
	LogSigningKey KeyPair   `json:"log_signing_key"`
}

var runtimeConfig *RuntimeConfig

func GetRuntimeConfig() *RuntimeConfig {
	return runtimeConfig
}

func GetAuditLogDir() string {
	appDataDir, err := os.UserConfigDir()
	if err != nil {
		log.Fatal().Err(err).Msg("failed to get user config dir")
	}
	return filepath.Join(appDataDir, "tangent", "auditlogs")
}

func CreateAuditLogDir() {
	dir := GetAuditLogDir()
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		if err := os.MkdirAll(dir, 0755); err != nil {
			log.Fatal().Err(err).Msg("failed to create audit log dir")
		}
	}
}

func GetRuntimeConfigDir() string {
	appDataDir, err := os.UserConfigDir()
	if err != nil {
		log.Fatal().Err(err).Msg("failed to get user config dir")
	}
	return filepath.Join(appDataDir, "tangent", "runtime")
}

func CreateRuntimeConfigDir() {
	dir := GetRuntimeConfigDir()
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		if err := os.MkdirAll(dir, 0755); err != nil {
			log.Fatal().Err(err).Msg("failed to create runtime dir")
		}
	}
}

func LoadRuntimeConfig() error {
	runtimeDir := GetRuntimeConfigDir()
	configPath := filepath.Join(runtimeDir, "runtime.json")

	runtimeConfig = &RuntimeConfig{}

	if _, err := os.Stat(configPath); err == nil {
		f, err := os.Open(configPath)
		if err != nil {
			log.Fatal().Err(err).Msg("failed to open runtime config file")
		}
		defer f.Close()

		if err := json.NewDecoder(f).Decode(runtimeConfig); err != nil {
			log.Fatal().Err(err).Msg("failed to decode runtime config")
		}
		return nil
	} else {
		runtimeConfig.TangentID = uuid.New()
		runtimeConfig.Registered = false
		if err := saveRuntimeConfig(); err != nil {
			log.Fatal().Err(err).Msg("failed to save runtime config")
		}
	}
	return nil
}

func RuntimeInit() {
	CreateRuntimeConfigDir()
	CreateAuditLogDir()
	LoadRuntimeConfig()
}

func RegisterTangent() error {
	if runtimeConfig.Registered {
		log.Info().Msg("tangent already registered")
		return nil
	}

	accessKey, err := createKeyPair()
	if err != nil {
		log.Fatal().Err(err).Msg("failed to create access key")
	}
	logKey, err := createKeyPair()
	if err != nil {
		log.Fatal().Err(err).Msg("failed to create log key")
	}
	tangentInfo := &srvtangent.TangentInfo{
		ID:                     runtimeConfig.TangentID,
		URL:                    GetURL(),
		PublicKeyAccessKey:     accessKey.PublicKey,
		PublicKeyLogSigningKey: logKey.PublicKey,
		Capabilities: []catcommon.RunnerID{
			catcommon.StdioRunnerID,
		},
	}

	client := getHTTPClient(&clientConfig{
		serverURL: Config().TansiveServer.GetURL(),
	})

	req, err := json.Marshal(tangentInfo)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to marshal tangent info")
	}

	opts := httpclient.RequestOptions{
		Method: http.MethodPost,
		Path:   "/tangents",
		Body:   req,
	}

	var location string
	retry.Do(func() error {
		_, location, err = client.DoRequest(opts)
		if err != nil {
			return err
		}
		return nil
	}, retry.Attempts(5),
		retry.Delay(1*time.Second),
		retry.DelayType(retry.BackOffDelay),
		retry.LastErrorOnly(true),
		retry.OnRetry(func(n uint, err error) {
			log.Error().Err(err).Msg("failed to register tangent")
		}))
	if err != nil {
		log.Fatal().Err(err).Msg("exhausted retries")
		return err
	}

	runtimeConfig.TangentID = uuid.MustParse(strings.TrimPrefix(string(location), "/tangents/"))
	runtimeConfig.Registered = true
	runtimeConfig.RegisteredAt = time.Now()
	runtimeConfig.AccessKey = accessKey
	runtimeConfig.LogSigningKey = logKey

	return saveRuntimeConfig()
}

func saveRuntimeConfig() error {
	runtimeDir := GetRuntimeConfigDir()
	configPath := filepath.Join(runtimeDir, "runtime.json")
	f, err := os.Create(configPath)
	if err != nil {
		log.Error().Err(err).Msg("failed to create runtime config file")
		return err
	}
	defer f.Close()
	if err := json.NewEncoder(f).Encode(runtimeConfig); err != nil {
		log.Error().Err(err).Msg("failed to encode runtime config")
		return err
	}
	return nil
}

func deleteRuntimeConfig() {
	runtimeDir := GetRuntimeConfigDir()
	configPath := filepath.Join(runtimeDir, "runtime.json")
	os.Remove(configPath)
}

func createKeyPair() (KeyPair, error) {
	// create ED25519 key pair
	pubKey, privKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		return KeyPair{}, err
	}
	return KeyPair{
		PrivateKey: privKey,
		PublicKey:  pubKey,
	}, nil
}

var isTestMode = false

func SetTestMode(testMode bool) {
	isTestMode = testMode
}

func getHTTPClient(config *clientConfig) httpclient.HTTPClientInterface {
	if isTestMode {
		c, err := httpclient.NewTestClient(config)
		if err != nil {
			return nil
		}
		return c
	}
	return httpclient.NewClient(config)
}

type clientConfig struct {
	token       string
	tokenExpiry time.Time
	serverURL   string
}

func (c *clientConfig) GetToken() string {
	return c.token
}

func (c *clientConfig) GetAPIKey() string {
	return ""
}

func (c *clientConfig) GetTokenExpiry() time.Time {
	return c.tokenExpiry
}

func (c *clientConfig) GetServerURL() string {
	return c.serverURL
}

func (c *clientConfig) GetSigningKey() (string, []byte) {
	return "", nil
}
