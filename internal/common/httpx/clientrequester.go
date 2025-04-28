package httpx

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"time"
)

type Requester interface {
	RequestMethod() (string, string)
}

func Fetch(baseURL string, reqObj Requester, respObj interface{}, timeout time.Duration, overrideTransport ...http.RoundTripper) error {
	method, _ := reqObj.RequestMethod()
	path, err := resolvePath(reqObj)
	if err != nil {
		return err
	}
	// trim ending slash in baseURL
	fullURL := strings.TrimRight(baseURL, "/") + path
	if method == http.MethodPost {
		return postRequest(fullURL, reqObj, respObj, timeout, overrideTransport...)
	} else if method == http.MethodGet {
		return getRequest(fullURL, reqObj, respObj, timeout, overrideTransport...)
	} else if method == http.MethodPut {
		return putRequest(fullURL, reqObj, respObj, timeout, overrideTransport...)
	} else if method == http.MethodDelete {
		return deleteRequest(fullURL, reqObj, respObj, timeout, overrideTransport...)
	}
	return errors.New("Fetch: method not supported")
}

func resolvePath(data Requester) (string, error) {
	// Marshal data into JSON
	jsonData, err := json.Marshal(data)
	if err != nil {
		return "", err
	}
	// Convert JSON to a map for easy access
	var dataMap map[string]interface{}
	err = json.Unmarshal(jsonData, &dataMap)
	if err != nil {
		return "", err
	}
	// Call RequestPath to get the template
	_, requestPathTemplate := data.RequestMethod()
	// Replace placeholders in the template
	re := regexp.MustCompile(`\{([^}]+)\}`)
	replacedPath := re.ReplaceAllStringFunc(requestPathTemplate, func(match string) string {
		key := match[1 : len(match)-1] // Remove the surrounding { and }
		if value, ok := dataMap[key]; ok {
			return fmt.Sprintf("%v", value)
		} else {
			return match
		}
	})
	// Check if all placeholders were replaced
	if strings.Contains(replacedPath, "{") || strings.Contains(replacedPath, "//") {
		return "", errors.New("unable to determine request path")
	}
	return replacedPath, nil
}
