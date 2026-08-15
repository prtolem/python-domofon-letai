"""Async Python client for the unofficial Domofon Letai API."""

from .client import DomofonLetaiClient, normalize_phone, normalize_sms_code
from .exceptions import (
    ApiError,
    AuthenticationError,
    CredentialStoreError,
    DomofonLetaiError,
    NotFoundError,
    PermissionDeniedError,
    ProtocolError,
    PushDependencyError,
    PushError,
    RateLimitError,
    StreamAuthenticationError,
    StreamError,
    StreamNotAvailableError,
    TransportError,
    ValidationError,
)
from .models import (
    Building,
    IncomingCallEvent,
    Intercom,
    SipSettings,
    StreamFormat,
    StreamSource,
)
from .push import (
    FcmCredentialStore,
    FileFcmCredentialStore,
    IncomingCallListener,
    IncomingCallListenerState,
)
from .streaming import MediaStream

__all__ = [
    "ApiError",
    "AuthenticationError",
    "Building",
    "CredentialStoreError",
    "DomofonLetaiClient",
    "DomofonLetaiError",
    "FcmCredentialStore",
    "FileFcmCredentialStore",
    "IncomingCallEvent",
    "IncomingCallListener",
    "IncomingCallListenerState",
    "Intercom",
    "MediaStream",
    "NotFoundError",
    "PermissionDeniedError",
    "ProtocolError",
    "PushDependencyError",
    "PushError",
    "RateLimitError",
    "SipSettings",
    "StreamAuthenticationError",
    "StreamError",
    "StreamFormat",
    "StreamNotAvailableError",
    "StreamSource",
    "TransportError",
    "ValidationError",
    "normalize_phone",
    "normalize_sms_code",
]

__version__ = "0.2.0"
