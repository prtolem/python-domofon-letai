"""Asynchronous client for the unofficial Domofon Letai HTTP API."""

from __future__ import annotations

import ssl
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from types import MappingProxyType
from typing import Any
from urllib.parse import urlparse

import httpx

from ._constants import (
    API_URL,
    DEFAULT_DEVICE_CODE,
    DEFAULT_DEVICE_OS_ID,
    DEFAULT_HEADERS,
    DEFAULT_TIMEOUT,
    MEDIA_HEADERS,
)
from .exceptions import (
    ApiError,
    AuthenticationError,
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

_PHONE_MIN = 70_000_000_000
_PHONE_MAX = 79_999_999_999
_SMS_CODE_LENGTH = 6


def normalize_phone(phone: str | int) -> str:
    """Normalize a Russian phone number to ``7XXXXXXXXXX``."""

    digits = "".join(character for character in str(phone) if character.isdigit())

    if len(digits) == 10:
        digits = "7" + digits
    elif len(digits) == 11 and digits.startswith("8"):
        digits = "7" + digits[1:]

    if (
        len(digits) != 11
        or not digits.isdigit()
        or not _PHONE_MIN <= int(digits) <= _PHONE_MAX
    ):
        raise ValidationError("phone must be a Russian number in 7XXXXXXXXXX format")

    return digits


def normalize_sms_code(code: str | int) -> str:
    """Validate and normalize a six-digit SMS code."""

    digits = "".join(character for character in str(code) if character.isdigit())
    if len(digits) != _SMS_CODE_LENGTH:
        raise ValidationError("SMS code must contain exactly six digits")
    return digits


class DomofonLetaiClient:
    """Async API client with non-buffering HLS and MPEG-TS access.

    The client owns HTTP clients it creates itself. Injected ``httpx.AsyncClient``
    instances remain owned by the caller and are never closed by this class.
    """

    def __init__(
        self,
        phone: str | int,
        *,
        access_token: str | None = None,
        timeout: float | httpx.Timeout = DEFAULT_TIMEOUT,
        media_verify: bool | ssl.SSLContext = True,
        api_client: httpx.AsyncClient | None = None,
        media_client: httpx.AsyncClient | None = None,
        device_code: str = DEFAULT_DEVICE_CODE,
    ) -> None:
        self._phone = normalize_phone(phone)
        self._access_token = access_token
        self._device_code = device_code
        self._closed = False
        self._intercoms: dict[int, Intercom] = {}

        self._owns_api_client = api_client is None
        self._owns_media_client = media_client is None

        self._api_client = api_client or httpx.AsyncClient(
            http1=True,
            http2=True,
            timeout=timeout,
        )

        if isinstance(timeout, httpx.Timeout):
            media_timeout = httpx.Timeout(
                connect=timeout.connect,
                read=None,
                write=timeout.write,
                pool=timeout.pool,
            )
        else:
            media_timeout = httpx.Timeout(
                connect=timeout,
                read=None,
                write=timeout,
                pool=timeout,
            )

        self._media_client = media_client or httpx.AsyncClient(
            http1=True,
            http2=False,
            timeout=media_timeout,
            verify=media_verify,
            follow_redirects=False,
        )

    @property
    def phone(self) -> str:
        """Normalized subscriber phone number."""

        return self._phone

    @property
    def access_token(self) -> str | None:
        """Current access token, if authentication has completed."""

        return self._access_token

    async def __aenter__(self) -> DomofonLetaiClient:
        self._ensure_open()
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        """Close HTTP clients owned by this instance."""

        if self._closed:
            return

        self._closed = True
        if self._owns_api_client:
            await self._api_client.aclose()
        if self._owns_media_client:
            await self._media_client.aclose()

    async def request_sms_code(self) -> None:
        """Ask Tattelecom to send an SMS authentication code.

        Starting a new authentication session may sign the official application out.
        This method is never retried automatically.
        """

        await self._request_json(
            "auth",
            method="POST",
            version="v2",
            body={"device_code": self._device_code, "phone": self._phone},
            authenticated=False,
        )

    async def confirm_sms_code(self, code: str | int) -> str:
        """Exchange an SMS code for an access token and retain it on the client."""

        data = await self._request_json(
            "auth/confirm-sms",
            method="POST",
            version="v2",
            body={
                "device_code": self._device_code,
                "phone": self._phone,
                "sms_code": normalize_sms_code(code),
                "device_os_id": DEFAULT_DEVICE_OS_ID,
            },
            authenticated=False,
        )

        token = data.get("access_token")
        if not isinstance(token, str) or not token:
            raise ProtocolError("authentication response has no access_token")

        self._access_token = token
        return token

    async def list_intercoms(self) -> list[Intercom]:
        """Fetch and parse all intercom panels available to the account."""

        data = await self._request_json("subscriber/gates", version="v2")
        if "gates" not in data:
            raise ProtocolError("subscriber/gates response has no gates field")

        raw_gates = data["gates"]
        if not isinstance(raw_gates, list):
            raise ProtocolError("subscriber/gates gates field is not a list")

        intercoms = [self._parse_intercom(raw_gate) for raw_gate in raw_gates]
        self._intercoms = {intercom.id: intercom for intercom in intercoms}
        return intercoms

    async def get_intercom(self, intercom_id: int, *, refresh: bool = True) -> Intercom:
        """Return one intercom, refreshing signed stream URLs by default."""

        if refresh or intercom_id not in self._intercoms:
            await self.list_intercoms()

        intercom = self._intercoms.get(intercom_id)
        if intercom is None:
            raise NotFoundError(f"intercom {intercom_id} was not found")
        return intercom

    async def open_door(self, intercom_id: int, *, screen_id: int = 1) -> None:
        """Ask the selected intercom to open its door.

        A successful API response confirms acceptance of the command, not a physical
        door-state change. This non-idempotent operation is never retried automatically.
        """

        await self._request_json(
            "gate/open-door",
            method="POST",
            version="v2",
            body={"gate_id": intercom_id, "data": {"screen_id": screen_id}},
        )

    async def get_stream_source(
        self,
        intercom_id: int,
        format: StreamFormat = StreamFormat.MPEG_TS,
        *,
        refresh: bool = True,
    ) -> StreamSource:
        """Return a fresh opaque URL for the requested stream format."""

        intercom = await self.get_intercom(intercom_id, refresh=refresh)
        source = intercom.mpeg_ts if format is StreamFormat.MPEG_TS else intercom.hls
        if source is None:
            raise StreamNotAvailableError(
                f"intercom {intercom_id} has no {format.value} stream"
            )
        return source

    @asynccontextmanager
    async def open_stream(
        self,
        intercom_id: int,
        format: StreamFormat = StreamFormat.MPEG_TS,
        *,
        refresh_url: bool = True,
    ) -> AsyncIterator[MediaStream]:
        """Open a non-buffered media response for an intercom.

        MPEG-TS is the default because it generally has lower latency than HLS. The
        media server is tried anonymously first and, only after a 401/403 response,
        retried once with the access token.
        """

        source = await self.get_stream_source(
            intercom_id,
            format,
            refresh=refresh_url,
        )
        response = await self._open_media_response(source.url)

        try:
            yield MediaStream(response)
        finally:
            await response.aclose()

    async def _request_json(
        self,
        path: str,
        *,
        method: str = "GET",
        version: str = "v1",
        body: Mapping[str, Any] | None = None,
        params: Mapping[str, Any] | None = None,
        authenticated: bool = True,
    ) -> dict[str, Any]:
        self._ensure_open()

        headers = dict(DEFAULT_HEADERS)
        if authenticated:
            headers["access-token"] = self._require_access_token()

        url = API_URL.format(version=version, path=path.lstrip("/"))
        try:
            response = await self._api_client.request(
                method,
                url,
                json=body,
                params=params,
                headers=headers,
            )
        except httpx.HTTPError as error:
            message = f"API request failed: {type(error).__name__}"
            raise TransportError(message) from error

        data: dict[str, Any] | None = None
        try:
            decoded = response.json()
            if isinstance(decoded, dict):
                data = decoded
        except ValueError:
            pass

        if data is None:
            if response.status_code >= 400:
                self._raise_api_error(response, {})
            raise ProtocolError("API returned a non-object or invalid JSON response")

        self._raise_api_error(response, data)
        return data

    @staticmethod
    def _raise_api_error(response: httpx.Response, data: Mapping[str, Any]) -> None:
        status_code = response.status_code
        error_code = data.get("error_code")
        embedded_status = DomofonLetaiClient._integer_or_none(data.get("status"))
        error_code_status = DomofonLetaiClient._integer_or_none(error_code)

        message_value = data.get("error_text") or data.get("message")
        message = (
            str(message_value)
            if message_value
            else f"Tattelecom API returned HTTP {status_code}"
        )
        details = {
            "status_code": status_code,
            "error_code": error_code,
            "retry_after": response.headers.get("retry-after"),
        }

        effective_status = status_code
        if effective_status < 400 and error_code_status is not None:
            effective_status = error_code_status
        if effective_status < 400 and embedded_status is not None:
            effective_status = embedded_status

        if effective_status == 401:
            raise AuthenticationError(message, **details)
        if effective_status == 403:
            raise PermissionDeniedError(message, **details)
        if effective_status == 404:
            raise NotFoundError(message, **details)
        if effective_status == 429:
            raise RateLimitError(message, **details)

        failed = (
            status_code >= 400
            or data.get("success") is False
            or error_code is not None
            or (embedded_status is not None and embedded_status >= 400)
        )
        if failed:
            raise ApiError(message, **details)

    async def _open_media_response(self, url: str) -> httpx.Response:
        self._ensure_open()
        self._validate_stream_url(url)

        attempts: list[dict[str, str]] = [dict(MEDIA_HEADERS)]
        if self._access_token:
            authenticated_headers = dict(MEDIA_HEADERS)
            authenticated_headers["access-token"] = self._access_token
            attempts.append(authenticated_headers)

        for index, headers in enumerate(attempts):
            try:
                request = self._media_client.build_request("GET", url, headers=headers)
                response = await self._media_client.send(request, stream=True)
            except httpx.HTTPError as error:
                raise TransportError(
                    f"media request failed: {type(error).__name__}"
                ) from error

            if response.status_code in (401, 403) and index + 1 < len(attempts):
                await response.aclose()
                continue

            if response.status_code in (401, 403):
                await response.aclose()
                raise StreamAuthenticationError(
                    "media server rejected stream access",
                    status_code=response.status_code,
                )

            if 300 <= response.status_code < 400:
                await response.aclose()
                raise StreamError(
                    "media server redirect was not followed for security reasons",
                    status_code=response.status_code,
                )

            if response.status_code >= 400:
                await response.aclose()
                raise StreamError(
                    f"media server returned HTTP {response.status_code}",
                    status_code=response.status_code,
                )

            return response

        raise StreamAuthenticationError("media server rejected stream access")

    def _parse_intercom(self, raw: Any) -> Intercom:
        if not isinstance(raw, dict):
            raise ProtocolError("gate entry is not an object")

        try:
            intercom_id = int(raw["gate_id"])
        except (KeyError, TypeError, ValueError) as error:
            raise ProtocolError("gate entry has an invalid gate_id") from error

        raw_buildings = raw.get("buildings") or []
        if not isinstance(raw_buildings, list):
            raise ProtocolError(f"gate {intercom_id} buildings field is not a list")

        buildings: list[Building] = []
        for raw_building in raw_buildings:
            if not isinstance(raw_building, dict):
                raise ProtocolError(f"gate {intercom_id} has an invalid building entry")
            try:
                building_id = int(raw_building["building_id"])
            except (KeyError, TypeError, ValueError) as error:
                raise ProtocolError(
                    f"gate {intercom_id} has a building with an invalid id"
                ) from error
            buildings.append(
                Building(
                    id=building_id,
                    address=str(raw_building.get("building_address") or ""),
                )
            )

        hls = self._parse_stream_source(
            intercom_id,
            StreamFormat.HLS,
            raw.get("stream_url"),
        )
        mpeg_ts = self._parse_stream_source(
            intercom_id,
            StreamFormat.MPEG_TS,
            raw.get("stream_url_mpeg"),
        )
        name = str(raw.get("gate_name") or "").strip() or f"Intercom {intercom_id}"
        sip_login_value = raw.get("sip_login")

        return Intercom(
            id=intercom_id,
            name=name,
            sip_login=str(sip_login_value) if sip_login_value is not None else None,
            muted=bool(raw.get("mute", False)),
            buildings=tuple(buildings),
            hls=hls,
            mpeg_ts=mpeg_ts,
            extra=MappingProxyType(dict(raw)),
        )

    @staticmethod
    def _parse_stream_source(
        intercom_id: int,
        format: StreamFormat,
        value: Any,
    ) -> StreamSource | None:
        if value is None or value == "":
            return None
        if not isinstance(value, str):
            raise ProtocolError(
                f"gate {intercom_id} has a non-string {format.value} stream URL"
            )
        DomofonLetaiClient._validate_stream_url(value)
        return StreamSource(intercom_id=intercom_id, format=format, url=value)

    @staticmethod
    def _validate_stream_url(url: str) -> None:
        parsed = urlparse(url)
        if parsed.scheme not in ("http", "https") or not parsed.hostname:
            raise ProtocolError("API returned an invalid media URL")

    def _require_access_token(self) -> str:
        if not self._access_token:
            raise AuthenticationError("an access token is required")
        return self._access_token

    def _ensure_open(self) -> None:
        if self._closed:
            raise RuntimeError("DomofonLetaiClient is closed")

    @staticmethod
    def _integer_or_none(value: Any) -> int | None:
        try:
            return int(value) if value is not None else None
        except (TypeError, ValueError):
            return None
