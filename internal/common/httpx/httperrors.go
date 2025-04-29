package httpx

import (
	"encoding/json"
	"net/http"

	"github.com/tansive/tansive-internal/internal/common/apperrors"
)

type Error struct {
	Description string `json:"description"`
	StatusCode  int    `json:"http_status_code"`
}

type errorRsp struct {
	Result int    `json:"result"`
	Error  string `json:"error"`
}

const Failure int = 0

func (e *Error) Send(w http.ResponseWriter) {
	if w != nil {
		rsp := &errorRsp{
			Result: Failure,
			Error:  e.Description,
		}
		// Encode the response struct as JSON and send it
		rspJson, err := json.Marshal(rsp)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("Unable to parse error"))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(e.StatusCode)
		w.Write(rspJson)
	}
}

func (e *Error) Error() string {
	return e.Description
}

func (current Error) Is(other error) bool {
	return current.Error() == other.Error()
}

func SendError(w http.ResponseWriter, err apperrors.Error) {
	if err == nil {
		return
	}
	statusCode := err.StatusCode()
	if statusCode == 0 {
		statusCode = http.StatusInternalServerError
	}
	httperror := &Error{
		StatusCode:  statusCode,
		Description: err.ErrorAll(),
	}
	httperror.Send(w)
}

// Common Errors

func ErrPostReqNotSupported() *Error {
	return &Error{
		Description: "POST Request Not Supported",
		StatusCode:  http.StatusMethodNotAllowed,
	}
}

func ErrGetReqNotSupported() *Error {
	return &Error{
		Description: "GET Request Not Supported",
		StatusCode:  http.StatusMethodNotAllowed,
	}
}

func ErrReqMethodNotSupported() *Error {
	return &Error{
		Description: "Request Method Not Supported",
		StatusCode:  http.StatusMethodNotAllowed,
	}
}

func ErrUnableToParseReqData() *Error {
	return &Error{
		Description: "Unable to parse request",
		StatusCode:  http.StatusBadRequest,
	}
}

func ErrUnableToReadRequest() *Error {
	return &Error{
		Description: "Unable to read request",
		StatusCode:  http.StatusBadRequest,
	}
}

func ErrApplicationError(err ...string) *Error {
	var s string
	if len(err) > 0 {
		s = err[0]
	} else {
		s = "Unable to process request"
	}
	return &Error{
		Description: s,
		StatusCode:  http.StatusInternalServerError,
	}
}

func ErrUnAuthorized(str ...string) *Error {
	var s string
	if len(str) > 0 {
		s = str[0]
	} else {
		s = "Unable to authenticate request"
	}
	return &Error{
		Description: s,
		StatusCode:  http.StatusUnauthorized,
	}
}

func ErrMissingKeyInRequest() *Error {
	return &Error{
		Description: "missing authentication information in request",
		StatusCode:  http.StatusUnauthorized,
	}
}

func ErrInvalidRequest(str ...string) *Error {
	var s string
	if len(str) > 0 {
		s = str[0]
	} else {
		s = "empty request values or invalid request"
	}
	return &Error{
		Description: s,
		StatusCode:  http.StatusBadRequest,
	}
}

func ErrInvalidTenantId() *Error {
	return &Error{
		Description: "Empty or invalid account id",
		StatusCode:  http.StatusBadRequest,
	}
}

func ErrInvalidProjectId() *Error {
	return &Error{
		Description: "Empty or invalid project id",
		StatusCode:  http.StatusBadRequest,
	}
}

func ErrInvalidNodeId() *Error {
	return &Error{
		Description: "Empty or invalid node id",
		StatusCode:  http.StatusBadRequest,
	}
}

func ErrInvalidCatalog() *Error {
	return &Error{
		Description: "Empty or invalid catalog",
		StatusCode:  http.StatusBadRequest,
	}
}

func ErrInvalidVariant() *Error {
	return &Error{
		Description: "Empty or invalid variant",
		StatusCode:  http.StatusBadRequest,
	}
}

func ErrInvalidNamespace() *Error {
	return &Error{
		Description: "Empty or invalid namespace",
		StatusCode:  http.StatusBadRequest,
	}
}

func ErrInvalidWorkspace() *Error {
	return &Error{
		Description: "Empty or invalid workspace",
		StatusCode:  http.StatusBadRequest,
	}
}

func ErrInvalidOnboardingKey() *Error {
	return &Error{
		Description: "Empty or invalid onboarding key",
		StatusCode:  http.StatusBadRequest,
	}
}

func ErrInvalidUser() *Error {
	return &Error{
		Description: "Empty or invalid user",
		StatusCode:  http.StatusUnauthorized,
	}
}
