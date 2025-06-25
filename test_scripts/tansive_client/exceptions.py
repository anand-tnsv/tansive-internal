"""Exceptions for the Tansive SDK."""


class TansiveError(Exception):
    """Base exception for all Tansive SDK errors."""

    pass


class TansiveConnectionError(TansiveError):
    """Raised when there is an error connecting to the Tansive service."""

    pass


class TansiveTimeoutError(TansiveError):
    """Raised when a request times out."""

    pass


class TansiveAPIError(TansiveError):
    """Raised when the API returns an error response."""

    def __init__(
        self, message: str, status_code: int = None, response_body: str = None
    ):
        super().__init__(message)
        self.status_code = status_code
        self.response_body = response_body


class TansiveRetryError(TansiveError):
    """Raised when all retries have been exhausted."""

    def __init__(self, message: str, last_error: Exception = None):
        super().__init__(message)
        self.last_error = last_error


class TansiveValidationError(TansiveError):
    """Raised when there are validation errors with the request."""

    pass
