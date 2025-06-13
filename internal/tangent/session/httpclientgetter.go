package session

import (
	"context"
	"sync"
	"time"

	"github.com/tansive/tansive-internal/internal/common/apperrors"
	"github.com/tansive/tansive-internal/internal/common/httpclient"
	"github.com/tansive/tansive-internal/internal/tangent/config"
	"github.com/tansive/tansive-internal/internal/tangent/runners"
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

var (
	tansiveSrvClientConfig *clientConfig
	tansiveSrvMutex        sync.RWMutex
)

func setTansiveSrvClient(ctx context.Context, config *clientConfig) apperrors.Error {
	_ = ctx
	tansiveSrvMutex.Lock()
	tansiveSrvClientConfig = config
	tansiveSrvMutex.Unlock()
	return nil
}

func getTansiveSrvClient() httpclient.HTTPClientInterface {
	tansiveSrvMutex.RLock()
	defer tansiveSrvMutex.RUnlock()
	return getHTTPClient(tansiveSrvClientConfig)
}

var isTestMode bool

func SetTestMode(testMode bool) {
	isTestMode = testMode
}

func Init() {
	setTansiveSrvClient(context.Background(), &clientConfig{
		token:       "some-token",
		tokenExpiry: time.Now().Add(1 * time.Hour),
		serverURL:   config.Config().TansiveServer.GetURL(),
	})
	runners.Init()
}
