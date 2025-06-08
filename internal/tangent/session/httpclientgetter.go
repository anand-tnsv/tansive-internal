package session

import (
	"time"

	"github.com/tansive/tansive-internal/internal/common/httpclient"
)

type clientConfig struct {
	token       string
	tokenExpiry time.Time
	serverURL   string
}

func (c *clientConfig) GetToken() string {
	return c.token
}

func (c *clientConfig) GetTokenExpiry() time.Time {
	return c.tokenExpiry
}

func (c *clientConfig) GetServerURL() string {
	return c.serverURL
}

func (c *clientConfig) GetAPIKey() string {
	return ""
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

var isTestMode bool

func SetTestMode(testMode bool) {
	isTestMode = testMode
}
