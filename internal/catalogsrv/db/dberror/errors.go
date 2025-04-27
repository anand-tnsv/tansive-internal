package dberror

import (
	"net/http"

	"github.com/tansive/tansive-internal/internal/common/apperrors"
)

/*
// dbError implements the apperrors.Error interface

	type dbError struct {
		msg string
		err error
	}

	func (e *dbError) Error() string {
		return e.msg
	}

	func (e *dbError) Unwrap() error {
		return e.err
	}

	func (e *dbError) Msg(msg string) apperrors.Error {
		return &dbError{
			msg: msg,
			err: e,
		}
	}

	func (e *dbError) MsgErr(msg string, err ...error) apperrors.Error {
		f := ""
		if e.err != nil {
			f = "%w "
		}
		for _, e := range err {
			_ = e
			f = f + "%w "
		}
		// trim the trailing space
		f = strings.TrimRight(f, " ")
		return &dbError{
			msg: msg,
			err: fmt.Errorf(f, e.Err, err),
		}
	}

	func (e *dbError) Err(err ...error) apperrors.Error {
		f := ""
		if e.err != nil {
			f = "%w "
		}
		for _, e := range err {
			_ = e
			f = f + "%w "
		}
		// trim the trailing space
		f = strings.TrimRight(f, " ")
		return &dbError{
			msg: e.msg,
			err: fmt.Errorf(f, e, err),
		}
	}

	func New(msg string) *dbError {
		return &dbError{
			msg: msg,
			err: nil,
		}
	}
*/
var (
	ErrDatabase                  apperrors.Error = apperrors.New("db error").SetStatusCode(http.StatusInternalServerError)
	ErrAlreadyExists             apperrors.Error = ErrDatabase.New("already exists").SetStatusCode(http.StatusConflict)
	ErrNotFound                  apperrors.Error = ErrDatabase.New("not found").SetStatusCode(http.StatusNotFound)
	ErrInvalidInput              apperrors.Error = ErrDatabase.New("invalid input").SetStatusCode(http.StatusBadRequest)
	ErrInvalidCatalog            apperrors.Error = ErrDatabase.New("invalid catalog").SetStatusCode(http.StatusBadRequest)
	ErrInvalidVariant            apperrors.Error = ErrDatabase.New("invalid variant").SetStatusCode(http.StatusBadRequest)
	ErrMissingTenantID           apperrors.Error = ErrInvalidInput.New("missing tenant ID").SetStatusCode(http.StatusBadRequest)
	ErrMissingProjecID           apperrors.Error = ErrInvalidInput.New("missing project ID").SetStatusCode(http.StatusBadRequest)
	ErrNoAncestorReferencesFound apperrors.Error = ErrDatabase.New("no ancestor references found").SetStatusCode(http.StatusBadRequest)
)
