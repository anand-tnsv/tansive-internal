package httpx

import (
	"encoding/json"
	"net/http"

	"github.com/rs/zerolog/log"
	"github.com/tansive/tansive-internal/internal/common/apperrors"
)

func GetRequestData(r *http.Request, data any) error {
	if r.Method != http.MethodPost && r.Method != http.MethodPut {
		return ErrReqMethodNotSupported()
	}
	if r.Body == nil {
		log.Ctx(r.Context()).Error().Msg("Empty request body")
		return ErrUnableToParseReqData()
	}
	if err := json.NewDecoder(r.Body).Decode(data); err != nil {
		return ErrUnableToParseReqData()
	}
	return nil
}

type Response struct {
	StatusCode  int
	Location    string //in case of http.StatusAccepted
	Response    any
	ContentType string
}

type RequestHandler func(r *http.Request) (*Response, error)

func WrapHttpRsp(handler RequestHandler) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rsp, err := handler(r)
		if err != nil {
			if httperror, ok := err.(*Error); ok {
				httperror.Send(w)
			} else if appErr, ok := err.(apperrors.Error); ok {
				statusCode := appErr.StatusCode()
				if statusCode == 0 {
					statusCode = http.StatusInternalServerError
				}
				httperror := &Error{
					StatusCode:  statusCode,
					Description: appErr.ErrorAll(),
				}
				httperror.Send(w)
			} else {
				ErrApplicationError(err.Error()).Send(w)
			}
			return
		}
		if rsp == nil {
			ErrApplicationError().Send(w)
			return
		}
		if rsp.ContentType == "" {
			rsp.ContentType = "application/json"
		}
		var location []string
		if rsp.Location != "" {
			location = append(location, rsp.Location)
		}
		if rsp.ContentType == "application/json" {
			SendJsonRsp(r.Context(), w, rsp.StatusCode, rsp.Response, location...)
		} else {
			ErrApplicationError("unsupported response type").Send(w)
		}
	})
}

type ResponseHandlerParam struct {
	Method  string
	Path    string
	Handler RequestHandler
}
