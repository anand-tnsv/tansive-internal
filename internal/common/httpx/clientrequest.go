package httpx

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"time"

	log "github.com/rs/zerolog/log"
)

type HTTPError struct {
	StatusCode int
	Status     string
	Message    string
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("HTTP %d: %s - %s", e.StatusCode, e.Status, e.Message)
}

func postRequest(url string, reqObj Requester, respObj interface{}, timeout time.Duration, overrideTransport ...http.RoundTripper) error {
	// Convert the request struct to JSON
	reqBody, err := json.Marshal(reqObj)
	if err != nil {
		return fmt.Errorf("failed to marshal request: %w", err)
	}

	// Retrieve the URL path from the request object
	request, err := http.NewRequest("POST", url, bytes.NewBuffer(reqBody))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")

	// Create an HTTP client and send the request
	var client *http.Client
	if timeout == 0 {
		timeout = 10 * time.Second
	}
	if len(overrideTransport) > 0 {
		client = &http.Client{
			Transport: overrideTransport[0],
			Timeout:   timeout,
		}
	} else {
		client = &http.Client{
			Timeout: timeout,
		}
	}
	response, err := client.Do(request)
	if err != nil {
		return fmt.Errorf("failed to send request: %w", err)
	}
	if response.Body == nil {
		return &HTTPError{
			StatusCode: response.StatusCode,
			Status:     response.Status,
			Message:    "empty response body",
		}
	}
	defer response.Body.Close()

	// Check for non-200 status codes
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		body, _ := io.ReadAll(response.Body)
		return &HTTPError{
			StatusCode: response.StatusCode,
			Status:     response.Status,
			Message:    string(body),
		}
	}

	// Decode the JSON response into the response struct
	err = json.NewDecoder(response.Body).Decode(respObj)
	if err != nil {
		return fmt.Errorf("failed to decode response: %w", err)
	}
	return nil
}

func putRequest(url string, reqObj Requester, respObj interface{}, timeout time.Duration, overrideTransport ...http.RoundTripper) error {
	// Convert the request struct to JSON
	reqBody, err := json.Marshal(reqObj)
	if err != nil {
		return err
	}
	// Retrieve the URL path from the request object
	request, err := http.NewRequest("PUT", url, bytes.NewBuffer(reqBody))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")

	// Create an HTTP client and send the request
	var client *http.Client
	if timeout == 0 {
		timeout = 10 * time.Second
	}
	if len(overrideTransport) > 0 {
		client = &http.Client{
			Transport: overrideTransport[0],
			Timeout:   timeout,
		}
	} else {
		client = &http.Client{
			Timeout: timeout,
		}
	}
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	if response.Body == nil {
		log.Error().Err(err).Msg("response body is nil")
		return fmt.Errorf("%s", response.Status)
	}
	defer response.Body.Close()
	// Decode the JSON response into the response struct
	err = json.NewDecoder(response.Body).Decode(respObj)
	if err != nil {
		log.Error().Err(err).Msg("unable to decode response")
		return fmt.Errorf("%s", response.Status)
	}
	return nil
}

func getRequest(url string, reqObj Requester, respObj interface{}, timeout time.Duration, overrideTransport ...http.RoundTripper) error {
	// Retrieve the URL path from the request object
	q, err := structToQueryString(reqObj)
	if err != nil {
		return err
	}
	url = fmt.Sprintf("%s?%s", url, q)
	request, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return err
	}
	// Create an HTTP client and send the request
	var client *http.Client
	if timeout == 0 {
		timeout = 10 * time.Second
	}
	if len(overrideTransport) > 0 {
		client = &http.Client{
			Transport: overrideTransport[0],
			Timeout:   timeout,
		}
	} else {
		client = &http.Client{
			Timeout: timeout,
		}
	}
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	if response.Body == nil {
		return fmt.Errorf("%s", response.Status)
	}
	defer response.Body.Close()

	// Decode the JSON response into the response struct
	err = json.NewDecoder(response.Body).Decode(respObj)
	if err != nil {
		return fmt.Errorf("%s", response.Status)
	}
	return nil
}

func deleteRequest(url string, reqObj Requester, respObj interface{}, timeout time.Duration, overrideTransport ...http.RoundTripper) error {
	// Retrieve the URL path from the request object
	q, err := structToQueryString(reqObj)
	if err != nil {
		return err
	}
	url = fmt.Sprintf("%s?%s", url, q)
	request, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		return err
	}
	// Create an HTTP client and send the request
	var client *http.Client
	if timeout == 0 {
		timeout = 10 * time.Second
	}
	if len(overrideTransport) > 0 {
		client = &http.Client{
			Transport: overrideTransport[0],
			Timeout:   timeout,
		}
	} else {
		client = &http.Client{
			Timeout: timeout,
		}
	}
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	if response.Body != nil && respObj != nil {
		defer response.Body.Close()
		// Decode the JSON response into the response struct
		json.NewDecoder(response.Body).Decode(respObj)
	}
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("%s", response.Status)
	}
	return nil
}

func structToQueryString(s interface{}) (string, error) {
	v := reflect.ValueOf(s)
	if v.Kind() != reflect.Ptr || v.Elem().Kind() != reflect.Struct {
		return "", fmt.Errorf("input must be a pointer to a struct")
	}

	v = v.Elem()
	t := v.Type()
	values := url.Values{}

	for i := 0; i < v.NumField(); i++ {
		field := t.Field(i)
		jsonTag := field.Tag.Get("json")
		if jsonTag == "" || jsonTag == "-" {
			continue
		}

		// Handle cases where the JSON tag includes options, e.g., "fieldname,omitempty"
		tagParts := strings.Split(jsonTag, ",")
		jsonKey := tagParts[0]

		fieldValue := v.Field(i)
		if fieldValue.Kind() == reflect.Ptr && fieldValue.IsNil() {
			// Skip nil pointer fields
			continue
		}

		// Convert the field value to a string
		// Here we use json.Marshal for simplicity, but need to customize this part
		// depending on how we want to handle different field types (e.g., time.Time)
		valueStr, err := json.Marshal(fieldValue.Interface())
		if err != nil {
			return "", fmt.Errorf("error marshaling field %s: %v", field.Name, err)
		}

		// Trim the quotes added by json.Marshal for basic types
		valueStrTrimmed := strings.Trim(string(valueStr), "\"")

		values.Add(jsonKey, valueStrTrimmed)
	}

	return values.Encode(), nil
}
