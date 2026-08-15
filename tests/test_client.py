from __future__ import annotations

import asyncio
import json
from collections.abc import Callable

import httpx
import pytest

from domofon_letai import (
    ApiError,
    AuthenticationError,
    DomofonLetaiClient,
    ProtocolError,
    RateLimitError,
    StreamAuthenticationError,
    StreamFormat,
    StreamNotAvailableError,
    ValidationError,
    normalize_phone,
    normalize_sms_code,
)

TOKEN = "secret-access-token"
PHONE = "79000000000"


class AsyncChunks(httpx.AsyncByteStream):
    def __init__(self, *chunks: bytes) -> None:
        self._chunks = chunks

    async def __aiter__(self):
        for chunk in self._chunks:
            yield chunk


def gate_payload(*, mpeg: str | None = "https://media.example/camera/mpegts") -> dict:
    return {
        "success": True,
        "gates": [
            {
                "gate_id": 42,
                "gate_name": "Подъезд №1",
                "sip_login": "G0042",
                "mute": False,
                "stream_url": "https://media.example/camera/index.m3u8",
                "stream_url_mpeg": mpeg,
                "buildings": [
                    {"building_id": 1, "building_address": "ул. Тестовая, д. 1"},
                    {"building_id": 2, "building_address": "ул. Тестовая, д. 2"},
                ],
                "unknown": "preserved",
            }
        ],
    }


def json_response(
    request: httpx.Request,
    data: dict,
    status: int = 200,
) -> httpx.Response:
    return httpx.Response(status, request=request, json=data)


def make_client(
    api_handler: Callable[[httpx.Request], httpx.Response],
    media_handler: Callable[[httpx.Request], httpx.Response] | None = None,
    *,
    token: str | None = TOKEN,
) -> tuple[DomofonLetaiClient, httpx.AsyncClient, httpx.AsyncClient]:
    api_client = httpx.AsyncClient(transport=httpx.MockTransport(api_handler))
    media_client = httpx.AsyncClient(
        transport=httpx.MockTransport(
            media_handler
            or (
                lambda request: httpx.Response(
                    200, request=request, stream=AsyncChunks(b"video")
                )
            )
        )
    )
    client = DomofonLetaiClient(
        PHONE,
        access_token=token,
        api_client=api_client,
        media_client=media_client,
    )
    return client, api_client, media_client


def run(coroutine):
    return asyncio.run(coroutine)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("+7 900 000-00-00", PHONE),
        ("8 (900) 000-00-00", PHONE),
        ("9000000000", PHONE),
        (79000000000, PHONE),
    ],
)
def test_normalize_phone(value: str | int, expected: str) -> None:
    assert normalize_phone(value) == expected


@pytest.mark.parametrize("value", ["123", "+1 555 000 0000", "", 1])
def test_reject_invalid_phone(value: str | int) -> None:
    with pytest.raises(ValidationError):
        normalize_phone(value)


def test_normalize_sms_code() -> None:
    assert normalize_sms_code("12 34-56") == "123456"
    with pytest.raises(ValidationError):
        normalize_sms_code("12345")


def test_authentication_flow_and_request_contract() -> None:
    requests: list[httpx.Request] = []

    def api_handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path == "/v2/auth":
            return json_response(request, {"success": True})
        return json_response(request, {"success": True, "access_token": TOKEN})

    async def scenario() -> None:
        client, api_client, media_client = make_client(api_handler, token=None)
        await client.request_sms_code()
        assert await client.confirm_sms_code("123456") == TOKEN
        assert client.access_token == TOKEN
        await client.aclose()
        assert not api_client.is_closed
        assert not media_client.is_closed
        await api_client.aclose()
        await media_client.aclose()

    run(scenario())

    assert [request.url.path for request in requests] == [
        "/v2/auth",
        "/v2/auth/confirm-sms",
    ]
    assert all("access-token" not in request.headers for request in requests)
    assert json.loads(requests[0].content) == {
        "device_code": "Android_empty_push_token",
        "phone": PHONE,
    }
    assert json.loads(requests[1].content)["sms_code"] == "123456"


def test_confirmation_requires_token_in_response() -> None:
    async def scenario() -> None:
        client, api_client, media_client = make_client(
            lambda request: json_response(request, {"success": True}),
            token=None,
        )
        with pytest.raises(ProtocolError):
            await client.confirm_sms_code("123456")
        await api_client.aclose()
        await media_client.aclose()

    run(scenario())


def test_parse_intercoms_and_stream_sources() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return json_response(request, gate_payload())

    async def scenario() -> None:
        client, api_client, media_client = make_client(handler)
        intercoms = await client.list_intercoms()
        intercom = intercoms[0]
        assert intercom.id == 42
        assert intercom.name == "Подъезд №1"
        assert intercom.addresses == ("ул. Тестовая, д. 1", "ул. Тестовая, д. 2")
        assert intercom.extra["unknown"] == "preserved"
        assert intercom.hls is not None
        assert intercom.mpeg_ts is not None
        source = await client.get_stream_source(42, refresh=False)
        assert source.format is StreamFormat.MPEG_TS
        await api_client.aclose()
        await media_client.aclose()

    run(scenario())
    assert requests[0].headers["access-token"] == TOKEN


def test_missing_gates_is_protocol_error() -> None:
    async def scenario() -> None:
        client, api_client, media_client = make_client(
            lambda request: json_response(request, {"success": True})
        )
        with pytest.raises(ProtocolError):
            await client.list_intercoms()
        await api_client.aclose()
        await media_client.aclose()

    run(scenario())


def test_open_door_contract() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return json_response(request, {"success": True})

    async def scenario() -> None:
        client, api_client, media_client = make_client(handler)
        await client.open_door(42, screen_id=3)
        await api_client.aclose()
        await media_client.aclose()

    run(scenario())
    assert requests[0].url.path == "/v2/gate/open-door"
    assert json.loads(requests[0].content) == {
        "gate_id": 42,
        "data": {"screen_id": 3},
    }


@pytest.mark.parametrize(
    ("status", "data", "error_type"),
    [
        (401, {}, AuthenticationError),
        (429, {"message": "slow down"}, RateLimitError),
        (200, {"success": False, "error_text": "failed"}, ApiError),
        (200, {"error_code": 401, "error_text": "expired"}, AuthenticationError),
    ],
)
def test_api_error_mapping(
    status: int,
    data: dict,
    error_type: type[Exception],
) -> None:
    async def scenario() -> None:
        client, api_client, media_client = make_client(
            lambda request: json_response(request, data, status)
        )
        with pytest.raises(error_type):
            await client.list_intercoms()
        await api_client.aclose()
        await media_client.aclose()

    run(scenario())


def test_invalid_success_body_is_protocol_error() -> None:
    async def scenario() -> None:
        client, api_client, media_client = make_client(
            lambda request: httpx.Response(200, request=request, text="not-json")
        )
        with pytest.raises(ProtocolError):
            await client.list_intercoms()
        await api_client.aclose()
        await media_client.aclose()

    run(scenario())


def test_stream_retries_with_token_and_yields_without_buffering() -> None:
    media_requests: list[httpx.Request] = []

    def api_handler(request: httpx.Request) -> httpx.Response:
        return json_response(request, gate_payload())

    def media_handler(request: httpx.Request) -> httpx.Response:
        media_requests.append(request)
        if "access-token" not in request.headers:
            return httpx.Response(401, request=request)
        return httpx.Response(
            200,
            request=request,
            headers={"content-type": "video/mp2t"},
            stream=AsyncChunks(b"chunk-one", b"chunk-two"),
        )

    async def scenario() -> None:
        client, api_client, media_client = make_client(api_handler, media_handler)
        async with client.open_stream(42) as stream:
            assert stream.content_type == "video/mp2t"
            chunks = [chunk async for chunk in stream.aiter_bytes(9)]
            assert b"".join(chunks) == b"chunk-onechunk-two"
        await api_client.aclose()
        await media_client.aclose()

    run(scenario())
    assert len(media_requests) == 2
    assert "access-token" not in media_requests[0].headers
    assert media_requests[1].headers["access-token"] == TOKEN


def test_stream_authentication_error() -> None:
    async def scenario() -> None:
        client, api_client, media_client = make_client(
            lambda request: json_response(request, gate_payload()),
            lambda request: httpx.Response(403, request=request),
        )
        with pytest.raises(StreamAuthenticationError):
            async with client.open_stream(42):
                pass
        await api_client.aclose()
        await media_client.aclose()

    run(scenario())


def test_missing_stream_format() -> None:
    async def scenario() -> None:
        client, api_client, media_client = make_client(
            lambda request: json_response(request, gate_payload(mpeg=None))
        )
        with pytest.raises(StreamNotAvailableError):
            await client.get_stream_source(42)
        await api_client.aclose()
        await media_client.aclose()

    run(scenario())
