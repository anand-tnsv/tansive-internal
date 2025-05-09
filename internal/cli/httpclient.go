package cli

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
)

// HTTPClient represents a client for making HTTP requests to the catalog server
type HTTPClient struct {
	config     *Config
	httpClient *http.Client
}

// NewHTTPClient creates a new HTTP client using the provided configuration
func NewHTTPClient(config *Config) *HTTPClient {
	return &HTTPClient{
		config:     config,
		httpClient: &http.Client{},
	}
}

// RequestOptions contains options for making HTTP requests
type RequestOptions struct {
	Method      string
	Path        string
	QueryParams map[string]string
	Body        []byte
}

// DoRequest makes an HTTP request with the given options
func (c *HTTPClient) DoRequest(opts RequestOptions) ([]byte, error) {
	// Build the URL with query parameters
	u, err := url.Parse(c.config.GetServerURL())
	if err != nil {
		return nil, fmt.Errorf("invalid server URL: %v", err)
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
		return nil, fmt.Errorf("failed to create request: %v", err)
	}

	// Set headers
	req.Header.Set("Content-Type", "application/json")
	if c.config.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.config.APIKey)
	}

	// Make the request
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %v", err)
	}
	defer resp.Body.Close()

	// Read the response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %v", err)
	}

	// Check for error status codes
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("request failed with status %d: %s", resp.StatusCode, string(body))
	}

	return body, nil
}

// CreateResource creates a new resource using the given JSON data
func (c *HTTPClient) CreateResource(resourceType string, data []byte, queryParams map[string]string) ([]byte, error) {
	opts := RequestOptions{
		Method:      http.MethodPost,
		Path:        resourceType,
		QueryParams: queryParams,
		Body:        data,
	}
	return c.DoRequest(opts)
}
