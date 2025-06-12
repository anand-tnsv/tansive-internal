package httpclient

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/tansive/tansive-internal/internal/catalogsrv/server"
	"github.com/tidwall/gjson"
)

// TestHTTPClient represents a test client for making HTTP requests directly to the catalog server
type TestHTTPClient struct {
	config     Configurator
	httpServer *server.CatalogServer
}

// NewTestClient creates a new test HTTP client using the provided configuration
func NewTestClient(config Configurator) (*TestHTTPClient, error) {
	s, err := server.CreateNewServer()
	if err != nil {
		return nil, fmt.Errorf("failed to create test server: %v", err)
	}
	s.MountHandlers()

	return &TestHTTPClient{
		config:     config,
		httpServer: s,
	}, nil
}

// DoRequest makes an HTTP request with the given options directly to the test server
func (c *TestHTTPClient) DoRequest(opts RequestOptions) ([]byte, string, error) {
	// Build the URL with query parameters
	u, err := url.Parse(c.config.GetServerURL())
	if err != nil {
		return nil, "", fmt.Errorf("invalid server URL: %v", err)
	}
	if u.Path == "" {
		u.Path = "/"
	}
	u.Path = path.Join(u.Path, opts.Path)

	// Add query parameters
	q := u.Query()
	for k, v := range opts.QueryParams {
		q.Set(k, v)
	}
	u.RawQuery = q.Encode()

	// Create the request
	req, err := http.NewRequest(opts.Method, u.String(), bytes.NewBuffer(opts.Body))
	if err != nil {
		return nil, "", fmt.Errorf("failed to create request: %v", err)
	}

	// Set headers
	req.Header.Set("Content-Type", "application/json")

	// Check if we have a valid current token
	if c.config.GetToken() != "" && !c.config.GetTokenExpiry().IsZero() {
		expiry := c.config.GetTokenExpiry()
		if time.Now().Before(expiry) {
			req.Header.Set("Authorization", "Bearer "+c.config.GetToken())
		} else {
			// Token expired or invalid, fall back to API key
			if c.config.GetAPIKey() != "" {
				req.Header.Set("Authorization", "Bearer "+c.config.GetAPIKey())
			}
		}
	} else if c.config.GetAPIKey() != "" {
		// No current token, use API key
		req.Header.Set("Authorization", "Bearer "+c.config.GetAPIKey())
	}

	// Create a response recorder
	rr := httptest.NewRecorder()

	// Serve the request directly
	c.httpServer.Router.ServeHTTP(rr, req)

	// Read the response body
	body := rr.Body.Bytes()

	// Check for error status codes
	if rr.Code >= 400 {
		var serverErr ServerError
		if err := json.Unmarshal(body, &serverErr); err == nil && serverErr.Error != "" {
			return nil, "", &HTTPError{
				StatusCode: rr.Code,
				Message:    serverErr.Error,
			}
		}
		return nil, "", &HTTPError{
			StatusCode: rr.Code,
			Message:    string(body),
		}
	}

	return body, rr.Header().Get("Location"), nil
}

// CreateResource creates a new resource using the given JSON data
func (c *TestHTTPClient) CreateResource(resourceType string, data []byte, queryParams map[string]string) ([]byte, string, error) {
	opts := RequestOptions{
		Method:      http.MethodPost,
		Path:        resourceType,
		QueryParams: queryParams,
		Body:        data,
	}
	return c.DoRequest(opts)
}

// GetResource retrieves a resource using the given resource name
func (c *TestHTTPClient) GetResource(resourceType string, resourceName string, queryParams map[string]string, objectType string) ([]byte, error) {
	// Clean the path components to avoid spurious slashes
	resourceType = strings.Trim(resourceType, "/")
	resourceName = strings.Trim(resourceName, "/")

	// Construct the path ensuring no double slashes
	path := strings.TrimSuffix(resourceType, "/")

	if objectType != "" {
		path = path + "/" + objectType
	}

	path = path + "/" + resourceName

	opts := RequestOptions{
		Method:      http.MethodGet,
		Path:        path,
		QueryParams: queryParams,
	}
	body, _, err := c.DoRequest(opts)
	return body, err
}

// DeleteResource deletes a resource using the given resource name
func (c *TestHTTPClient) DeleteResource(resourceType string, resourceName string, queryParams map[string]string, objectType string) error {
	resourceType = strings.Trim(resourceType, "/")
	resourceName = strings.Trim(resourceName, "/")

	// Construct the path ensuring no double slashes
	path := strings.TrimSuffix(resourceType, "/")

	if objectType != "" {
		path = path + "/" + objectType
	}

	path = path + "/" + resourceName

	opts := RequestOptions{
		Method:      http.MethodDelete,
		Path:        path,
		QueryParams: queryParams,
	}
	_, _, err := c.DoRequest(opts)
	return err
}

// UpdateResource updates an existing resource using the given JSON data
func (c *TestHTTPClient) UpdateResource(resourceType string, data []byte, queryParams map[string]string, objectType string) ([]byte, error) {
	// Get the resource name from metadata.name
	resourceName := gjson.GetBytes(data, "metadata.name").String()
	if resourceName == "" {
		return nil, fmt.Errorf("metadata.name is required for update")
	}

	// Clean the path components to avoid spurious slashes
	resourceType = strings.Trim(resourceType, "/")
	resourceName = strings.Trim(resourceName, "/")

	// Construct the path ensuring no double slashes
	path := strings.TrimSuffix(resourceType, "/")

	if objectType != "" {
		path = path + "/" + objectType
	}

	path = path + "/" + resourceName

	opts := RequestOptions{
		Method:      http.MethodPut,
		Path:        path,
		QueryParams: queryParams,
		Body:        data,
	}
	body, _, err := c.DoRequest(opts)
	return body, err
}

// UpdateResourceValue updates a resource value using the given JSON data
func (c *TestHTTPClient) UpdateResourceValue(resourcePath string, data []byte, queryParams map[string]string) ([]byte, error) {
	opts := RequestOptions{
		Method:      http.MethodPut,
		Path:        resourcePath,
		QueryParams: queryParams,
		Body:        data,
	}
	body, _, err := c.DoRequest(opts)
	return body, err
}

// ListResources lists resources of a specific type
func (c *TestHTTPClient) ListResources(resourceType string, queryParams map[string]string) ([]byte, error) {
	opts := RequestOptions{
		Method:      http.MethodGet,
		Path:        resourceType,
		QueryParams: queryParams,
	}
	body, _, err := c.DoRequest(opts)
	return body, err
}
