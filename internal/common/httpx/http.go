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

type WriteChunksFunc func(w http.ResponseWriter) error

type Response struct {
	StatusCode  int
	Location    string //in case of http.StatusAccepted
	Response    any
	ContentType string
	Chunked     bool // indicates if the response should be sent as chunked
	WriteChunks WriteChunksFunc
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
		if rsp.Chunked {
			if rsp.WriteChunks == nil {
				ErrApplicationError("unable to write chunks").Send(w)
				return
			}
			w.Header().Set("Content-Type", rsp.ContentType)
			w.Header().Set("Transfer-Encoding", "chunked")
			w.WriteHeader(rsp.StatusCode)
			if err := rsp.WriteChunks(w); err != nil {
				log.Ctx(r.Context()).Error().Err(err).Msg("Error writing chunk")
				return
			}
			return
		}

		if rsp.ContentType == "" {
			rsp.ContentType = "application/json"
		}
		var location []string
		if rsp.Location != "" {
			location = append(location, rsp.Location)
		}
		switch rsp.ContentType {
		case "application/json":
			SendJsonRsp(r.Context(), w, rsp.StatusCode, rsp.Response, location...)
		case "text/plain":
			w.Header().Set("Content-Type", "text/plain")
			if rsp.StatusCode == http.StatusCreated && len(location) > 0 {
				w.Header().Set("Location", location[0])
			}
			w.WriteHeader(rsp.StatusCode)
			w.Write([]byte(rsp.Response.(string)))
		default:
			ErrApplicationError("unsupported response type").Send(w)
		}
	})
}

// StreamResponse represents a streaming response that can write multiple chunks
type StreamResponse struct {
	StatusCode  int
	ContentType string
	WriteChunk  func(w http.ResponseWriter) error
}

// StreamHandler is a function that handles streaming responses
type StreamHandler func(r *http.Request) (*StreamResponse, error)

// WrapStreamHandler wraps a StreamHandler to handle HTTP streaming responses
func WrapStreamHandler(handler StreamHandler) http.HandlerFunc {
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

		// Set up streaming response
		w.Header().Set("Content-Type", rsp.ContentType)
		w.Header().Set("Transfer-Encoding", "chunked")
		w.WriteHeader(rsp.StatusCode)

		// Create a flusher to ensure chunks are sent immediately
		flusher, ok := w.(http.Flusher)
		if !ok {
			ErrApplicationError("streaming not supported").Send(w)
			return
		}

		// Write chunks until WriteChunk returns an error or nil
		for {
			if err := rsp.WriteChunk(w); err != nil {
				log.Ctx(r.Context()).Error().Err(err).Msg("Error writing chunk")
				return
			}
			flusher.Flush()
		}
	})
}
