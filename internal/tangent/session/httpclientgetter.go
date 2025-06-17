package session

import (
	"time"

	"github.com/tansive/tansive-internal/internal/common/httpclient"
	"github.com/tansive/tansive-internal/internal/tangent/config"
	"github.com/tansive/tansive-internal/internal/tangent/runners"
)

type clientConfig struct {
	signingKey   []byte
	signingKeyID string
	token        string
	tokenExpiry  time.Time
	serverURL    string
}

func (c *clientConfig) GetToken() string {
	return c.token
}

func (c *clientConfig) GetTokenExpiry() time.Time {
	return c.tokenExpiry
}

func (c *clientConfig) GetAPIKey() string {
	return ""
}

func (c *clientConfig) GetServerURL() string {
	return c.serverURL
}

func (c *clientConfig) GetSigningKey() (string, []byte) {
	if len(c.signingKey) == 0 || c.signingKeyID == "" {
		return "", nil
	}
	return c.signingKeyID, c.signingKey
}

func getHTTPClient(clientConfig *clientConfig) httpclient.HTTPClientInterface {
	runtimeConfig := config.GetRuntimeConfig()
	if runtimeConfig != nil && runtimeConfig.Registered {
		clientConfig.signingKey = runtimeConfig.AccessKey.PrivateKey
		clientConfig.signingKeyID = runtimeConfig.TangentID.String()
	}
	if isTestMode {
		c, err := httpclient.NewTestClient(clientConfig)
		if err != nil {
			return nil
		}
		return c
	}
	return httpclient.NewClient(clientConfig)
}

var isTestMode bool

func SetTestMode(testMode bool) {
	isTestMode = testMode
}

func Init() {
	runners.Init()
}
