package apperrors

// appError implements the apperrors.Error interface
type appError struct {
	msg           string
	base          Error
	wrappedErrors []error
	statuscode    int
	expandError   bool
	prefix        string
	suffix        string
}

func (e *appError) Error() string {
	if e.prefix != "" {
		e.msg = e.prefix + ": " + e.msg
	}
	if e.suffix != "" {
		e.msg += ": " + e.suffix
	}
	return e.msg
}

func (e *appError) ErrorAll() string {
	if !e.expandError {
		return e.msg
	}
	var msg string
	for _, err := range e.wrappedErrors {
		msg += err.Error() + ";"
	}
	if len(msg) > 0 {
		// remove the last ;
		msg = msg[:len(msg)-1]
		msg = e.msg + ": " + msg
	} else {
		msg = e.msg
	}

	return msg
}

func (e *appError) Unwrap() []error {
	return e.wrappedErrors
}

func (e *appError) New(msg string) Error {
	return &appError{
		msg:           msg,
		statuscode:    e.statuscode,
		base:          e,
		wrappedErrors: nil,
	}
}

func (e *appError) Msg(msg string) Error {
	e.msg = msg
	return e
}

func (e *appError) Prefix(prefix string) Error {
	e.prefix = prefix
	return e
}

func (e *appError) Suffix(suffix string) Error {
	e.suffix = suffix
	return e
}

func (e *appError) MsgErr(msg string, err ...error) Error {
	e.msg = msg
	e.wrappedErrors = append(e.wrappedErrors, err...)
	return e
}

func (e *appError) Err(err ...error) Error {
	e.wrappedErrors = append(e.wrappedErrors, err...)
	return e
}

func (e *appError) Is(target error) bool {
	if e == target || e.base == target {
		return true
	}
	if e.base != nil && e.base.Is(target) {
		return true
	}
	for _, err := range e.wrappedErrors {
		if err == target {
			return true
		}
	}
	return false
}

func (e *appError) SetExpandError(expand bool) Error {
	e.expandError = expand
	return e
}

func (e *appError) SetStatusCode(code int) Error {
	e.statuscode = code
	return e
}

func (e *appError) StatusCode() int {
	return e.statuscode
}

func New(msg string) Error {
	return &appError{
		msg:           msg,
		base:          nil,
		wrappedErrors: nil,
	}
}
