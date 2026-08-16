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


class SipError(DomofonLetaiError):
    """Base error for experimental SIP call signaling."""


class SipMetadataError(SipError):
    """A push event does not contain safe SIP connection metadata."""


class SipTransportError(SipError):
    """The SIP TLS connection failed."""


class SipAuthenticationError(SipError):
    """SIP registration credentials or a digest challenge were rejected."""


class SipProtocolError(SipError):
    """A malformed or unsupported SIP message was received."""


class SipCallMismatchError(SipProtocolError):
    """A SIP INVITE does not match the triggering push event."""


class SipCallStateError(SipError):
    """A call operation is invalid for the current signaling state."""


class SipTimeoutError(SipError):
    """A SIP operation exceeded its configured timeout."""


class OpenDoorAndEndError(SipError):
    """Door opening and SIP termination did not both complete successfully."""

    def __init__(
        self,
        message: str,
        *,
        door_request_succeeded: bool,
        sip_end_succeeded: bool,
        door_error: BaseException | None = None,
        sip_error: BaseException | None = None,
    ) -> None:
        super().__init__(message)
        self.door_request_succeeded = door_request_succeeded
        self.sip_end_succeeded = sip_end_succeeded
        self.door_error = door_error
        self.sip_error = sip_error
