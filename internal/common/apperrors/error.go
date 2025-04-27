package apperrors

type Error interface {
	Error() string
	ErrorAll() string
	New(msg string) Error
	MsgErr(msg string, err ...error) Error
	Msg(msg string) Error
	Prefix(prefix string) Error
	Suffix(suffix string) Error
	Err(err ...error) Error
	Unwrap() []error
	Is(target error) bool
	SetExpandError(expand bool) Error
	SetStatusCode(code int) Error
	StatusCode() int
}
