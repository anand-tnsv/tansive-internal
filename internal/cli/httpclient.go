package cli

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/tidwall/gjson"
)

// ServerError represents an error response from the server
type ServerError struct {
	Result int    `json:"result"`
	Error  string `json:"error"`
}

// HTTPError represents an error response from the server with a status code
type HTTPError struct {
	StatusCode int
	Message    string
}

func (e *HTTPError) Error() string {
	return e.Message
}

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
func (c *HTTPClient) DoRequest(opts RequestOptions) ([]byte, string, error) {
	// Build the URL with query parameters
	u, err := url.Parse(c.config.GetServerURL())
	if err != nil {
		return nil, "", fmt.Errorf("invalid server URL: %v", err)
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
	if c.config.CurrentToken != "" && c.config.TokenExpiry != "" {
		expiry, err := time.Parse(time.RFC3339, c.config.TokenExpiry)
		if err == nil && time.Now().Before(expiry) {
			req.Header.Set("Authorization", "Bearer "+c.config.CurrentToken)
		} else {
			// Token expired or invalid, fall back to API key
			if c.config.APIKey != "" {
				req.Header.Set("Authorization", "Bearer "+c.config.APIKey)
			}
		}
	} else if c.config.APIKey != "" {
		// No current token, use API key
		req.Header.Set("Authorization", "Bearer "+c.config.APIKey)
	}

	// Make the request
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, "", fmt.Errorf("request failed: %v", err)
	}
	defer resp.Body.Close()

	// Read the response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, "", fmt.Errorf("failed to read response body: %v", err)
	}

	// Check for error status codes
	if resp.StatusCode >= 400 {
		var serverErr ServerError
		if err := json.Unmarshal(body, &serverErr); err == nil && serverErr.Error != "" {
			return nil, "", &HTTPError{
				StatusCode: resp.StatusCode,
				Message:    serverErr.Error,
			}
		}
		return nil, "", &HTTPError{
			StatusCode: resp.StatusCode,
			Message:    string(body),
		}
	}

	return body, resp.Header.Get("Location"), nil
}

// CreateResource creates a new resource using the given JSON data
func (c *HTTPClient) CreateResource(resourceType string, data []byte, queryParams map[string]string) ([]byte, string, error) {
	opts := RequestOptions{
		Method:      http.MethodPost,
		Path:        resourceType,
		QueryParams: queryParams,
		Body:        data,
	}
	return c.DoRequest(opts)
}

// GetResource retrieves a resource using the given resource name
func (c *HTTPClient) GetResource(resourceType string, resourceName string, queryParams map[string]string, objectType string) ([]byte, error) {
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
func (c *HTTPClient) DeleteResource(resourceType string, resourceName string, queryParams map[string]string) error {
	resourceType = strings.Trim(resourceType, "/")
	resourceName = strings.Trim(resourceName, "/")

	path := strings.TrimSuffix(resourceType, "/") + "/" + strings.TrimPrefix(resourceName, "/")

	if resourceType == "resources" {
		path = path + ":definition"
	}

	opts := RequestOptions{
		Method:      http.MethodDelete,
		Path:        path,
		QueryParams: queryParams,
	}
	_, _, err := c.DoRequest(opts)
	return err
}

// UpdateResource updates an existing resource using the given JSON data
func (c *HTTPClient) UpdateResource(resourceType string, data []byte, queryParams map[string]string) ([]byte, error) {
	// Get the resource name from metadata.name
	resourceName := gjson.GetBytes(data, "metadata.name").String()
	if resourceName == "" {
		return nil, fmt.Errorf("metadata.name is required for update")
	}

	// Clean the path components to avoid spurious slashes
	resourceType = strings.Trim(resourceType, "/")
	resourceName = strings.Trim(resourceName, "/")

	// Construct the path ensuring no double slashes
	path := strings.TrimSuffix(resourceType, "/") + "/" + strings.TrimPrefix(resourceName, "/")

	if resourceType == "resources" {
		path = path + ":definition"
	}

	opts := RequestOptions{
		Method:      http.MethodPut,
		Path:        path,
		QueryParams: queryParams,
		Body:        data,
	}
	body, _, err := c.DoRequest(opts)
	return body, err
}

// ListResources lists resources of a specific type
func (c *HTTPClient) ListResources(resourceType string, queryParams map[string]string) ([]byte, error) {
	opts := RequestOptions{
		Method:      http.MethodGet,
		Path:        resourceType,
		QueryParams: queryParams,
	}
	body, _, err := c.DoRequest(opts)
	return body, err
}
