package httpclient

import (
	"io"
)

// HTTPClientInterface defines the interface for HTTP client implementations
type HTTPClientInterface interface {
	// DoRequest makes an HTTP request with the given options
	DoRequest(opts RequestOptions) ([]byte, string, error)

	// StreamRequest makes an HTTP request with the given options and streams the response
	StreamRequest(opts RequestOptions) (io.ReadCloser, error)

	// CreateResource creates a new resource using the given JSON data
	CreateResource(resourceType string, data []byte, queryParams map[string]string) ([]byte, string, error)

	// GetResource retrieves a resource using the given resource name
	GetResource(resourceType string, resourceName string, queryParams map[string]string, objectType string) ([]byte, error)

	// DeleteResource deletes a resource using the given resource name
	DeleteResource(resourceType string, resourceName string, queryParams map[string]string, objectType string) error

	// UpdateResource updates an existing resource using the given JSON data
	UpdateResource(resourceType string, data []byte, queryParams map[string]string, objectType string) ([]byte, error)

	// UpdateResourceValue updates a resource at a specific path
	UpdateResourceValue(resourcePath string, data []byte, queryParams map[string]string) ([]byte, error)

	// ListResources lists resources of a specific type
	ListResources(resourceType string, queryParams map[string]string) ([]byte, error)
}

// Verify that the HTTPClient and TestHTTPClient implement the HTTPClientInterface
var _ HTTPClientInterface = &HTTPClient{}
var _ HTTPClientInterface = &TestHTTPClient{}
