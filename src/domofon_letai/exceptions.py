"""Exceptions raised by :mod:`domofon_letai`."""

from __future__ import annotations


class DomofonLetaiError(Exception):
    """Base class for every library-specific error."""


class ValidationError(DomofonLetaiError, ValueError):
    """User input is invalid and no request was sent."""


class TransportError(DomofonLetaiError):
    """A network, timeout, DNS, or TLS error occurred."""


class ProtocolError(DomofonLetaiError):
    """The server returned a successful but malformed response."""


class ApiError(DomofonLetaiError):
    """The Tattelecom API rejected a request."""

    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        error_code: int | str | None = None,
        retry_after: str | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.error_code = error_code
        self.retry_after = retry_after


class AuthenticationError(ApiError):
    """The access token is missing, expired, or revoked."""


class PermissionDeniedError(ApiError):
    """The account is not allowed to perform the operation."""


class NotFoundError(ApiError):
    """The requested API resource does not exist."""


class RateLimitError(ApiError):
    """The API rate limit was reached."""


class StreamError(DomofonLetaiError):
    """A media stream could not be opened or consumed."""

    def __init__(self, message: str, *, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code


class StreamNotAvailableError(StreamError):
    """The intercom has no URL for the requested stream format."""


class StreamAuthenticationError(StreamError):
    """The media server rejected both anonymous and authenticated access."""


class PushError(DomofonLetaiError):
    """Incoming-call notification support failed."""


class PushDependencyError(PushError, ImportError):
    """The optional push dependency is not installed."""


class CredentialStoreError(PushError):
    """Push credentials could not be loaded or stored safely."""
