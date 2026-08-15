"""Async Python client for the unofficial Domofon Letai API."""

from .client import DomofonLetaiClient, normalize_phone, normalize_sms_code
from .exceptions import (
    ApiError,
    AuthenticationError,
    DomofonLetaiError,
    NotFoundError,
    PermissionDeniedError,
    ProtocolError,
    RateLimitError,
    StreamAuthenticationError,
    StreamError,
    StreamNotAvailableError,
    TransportError,
    ValidationError,
)
from .models import Building, Intercom, StreamFormat, StreamSource
from .streaming import MediaStream

__all__ = [
    "ApiError",
    "AuthenticationError",
    "Building",
    "DomofonLetaiClient",
    "DomofonLetaiError",
    "Intercom",
    "MediaStream",
    "NotFoundError",
    "PermissionDeniedError",
    "ProtocolError",
    "RateLimitError",
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

__version__ = "0.1.0"
