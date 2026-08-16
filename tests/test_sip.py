from __future__ import annotations

import asyncio
import json
import ssl
from collections.abc import Awaitable
from datetime import datetime, timezone
from types import MappingProxyType
from typing import Any

import httpx
import pytest

from domofon_letai import (
    DomofonLetaiClient,
    IncomingCallEvent,
    SipCallState,
    SipCallStateError,
    SipMetadataError,
    SipSettings,
    SipTimeoutError,
)
from domofon_letai.calls import SipConnection, connect_sip_call
from domofon_letai.exceptions import SipProtocolError
from domofon_letai.sip import (
    RegisterBuilder,
    SipMessage,
    SipResponseBuilder,
    SipStreamReader,
    build_inactive_sdp_answer,
)

PHONE = "79000000000"
TOKEN = "access-token"
HOST = "dmf-proxy01.tattelecom.ru"
CALL_ID = "call-from-push"
INVITE_SDP = (
    b"v=0\r\n"
    b"o=root 1 1 IN IP4 192.0.2.10\r\n"
    b"s=Talk\r\n"
    b"c=IN IP4 192.0.2.10\r\n"
    b"t=0 0\r\n"
    b"m=audio 40564 RTP/AVP 8 101\r\n"
    b"a=rtpmap:8 PCMA/8000\r\n"
    b"m=video 40378 RTP/AVP 99\r\n"
    b"a=rtpmap:99 H264/90000\r\n"
)


def run(awaitable: Awaitable[Any]) -> Any:
    return asyncio.run(awaitable)


def incoming_event(*, call_id: str = CALL_ID) -> IncomingCallEvent:
    return IncomingCallEvent(
        received_at=datetime.now(timezone.utc),
        message_id="persistent-id",
        call_id=call_id,
        notification_uuid="notification-id",
        sip_login="G17126",
        sip_address=HOST,
        sip_port=9741,
        sip_transport="tls",
        raw_data=MappingProxyType({}),
    )


def sip_settings(*, address: str = HOST) -> SipSettings:
    return SipSettings(
        address=address,
        port=9740,
        login="D000000",
        password="sip-secret",
        registration_expires=60,
    )


def invite_message(*, call_id: str = CALL_ID) -> SipMessage:
    return SipMessage(
        "INVITE sip:D000000@192.0.2.20:50000;transport=tls SIP/2.0",
        (
            (
                "Via",
                "SIP/2.0/TLS 192.0.2.10:9741;branch=z9hG4bK-remote;rport",
            ),
            ("Max-Forwards", "70"),
            ("From", '"G17126" <sip:G17126@192.0.2.10>;tag=remote-tag'),
            ("To", "<sip:D000000@192.0.2.20;transport=tls>"),
            ("Contact", "<sip:G17126@192.0.2.10:9741;transport=tls>"),
            ("Call-ID", call_id),
            ("CSeq", "102 INVITE"),
            ("Content-Type", "application/sdp"),
            ("Content-Length", str(len(INVITE_SDP))),
        ),
        INVITE_SDP,
    )


class FakeSipConnection:
    local_host = "192.0.2.20"
    local_port = 50000

    def __init__(
        self,
        invite: SipMessage | None = None,
        *,
        auto_ack: bool = True,
        auto_bye_response: bool = True,
        challenge_deregister: bool = False,
        early_invite_cancel: bool = False,
        fail_bye_send: bool = False,
    ) -> None:
        self.invite = invite or invite_message()
        self.auto_ack = auto_ack
        self.auto_bye_response = auto_bye_response
        self.challenge_deregister = challenge_deregister
        self.early_invite_cancel = early_invite_cancel
        self.fail_bye_send = fail_bye_send
        self.sent: list[SipMessage] = []
        self.responses: asyncio.Queue[SipMessage] = asyncio.Queue()
        self.register_count = 0
        self.closed = False

    async def send(self, message: SipMessage | bytes) -> None:
        assert isinstance(message, SipMessage)
        self.sent.append(message)

        if message.method == "REGISTER":
            self.register_count += 1
            if self.register_count == 1:
                await self.responses.put(
                    SipMessage(
                        "SIP/2.0 401 Unauthorized",
                        (
                            *self._transaction_headers(message),
                            (
                                "WWW-Authenticate",
                                'Digest algorithm=MD5, realm="test", nonce="nonce"',
                            ),
                            ("Content-Length", "0"),
                        ),
                    )
                )
            else:
                status = (
                    401
                    if self.challenge_deregister and self.register_count == 3
                    else 200
                )
                reason = "Unauthorized" if status == 401 else "OK"
                extra_headers: tuple[tuple[str, str], ...] = ()
                if status == 401:
                    extra_headers = (
                        (
                            "WWW-Authenticate",
                            'Digest algorithm=MD5, realm="test", nonce="fresh"',
                        ),
                    )
                response = SipMessage(
                    f"SIP/2.0 {status} {reason}",
                    (
                        *self._transaction_headers(message),
                        *extra_headers,
                        ("Content-Length", "0"),
                    ),
                )
                if self.register_count == 2 and self.early_invite_cancel:
                    await self.responses.put(self.invite)
                    await self.responses.put(cancel_message(self.invite))
                await self.responses.put(response)
                if self.register_count == 2 and not self.early_invite_cancel:
                    await self.responses.put(self.invite)
        elif (
            self.auto_ack
            and message.status_code in (200, 487, 603)
            and message.cseq_method == "INVITE"
        ):
            await self.responses.put(
                SipMessage(
                    "ACK sip:D000000@192.0.2.20:50000;transport=tls SIP/2.0",
                    (
                        ("Via", self.invite.get_header("Via") or ""),
                        ("From", self.invite.get_header("From") or ""),
                        (
                            "To",
                            message.get_header("To") or "",
                        ),
                        ("Call-ID", self.invite.call_id or ""),
                        ("CSeq", "102 ACK"),
                        ("Content-Length", "0"),
                    ),
                )
            )
        elif message.method == "BYE" and self.fail_bye_send:
            raise SipProtocolError("forced BYE send failure")
        elif message.method == "BYE" and self.auto_bye_response:
            await self.responses.put(
                SipMessage(
                    "SIP/2.0 200 OK",
                    (
                        *self._transaction_headers(message),
                        ("Content-Length", "0"),
                    ),
                )
            )

    async def receive(self) -> SipMessage:
        return await self.responses.get()

    async def close(self) -> None:
        self.closed = True

    @staticmethod
    def _transaction_headers(message: SipMessage) -> tuple[tuple[str, str], ...]:
        names = ("Via", "From", "To", "Call-ID", "CSeq")
        return tuple(
            (name, message.get_header(name) or "")
            for name in names
        )


class FakeConnectionFactory:
    def __init__(self, connection: FakeSipConnection) -> None:
        self.connection = connection
        self.calls: list[tuple[str, int]] = []

    async def __call__(
        self,
        host: str,
        port: int,
        _context: ssl.SSLContext,
        _timeout: float | None,
    ) -> SipConnection:
        self.calls.append((host, port))
        return self.connection


def api_client() -> tuple[DomofonLetaiClient, httpx.AsyncClient, list[httpx.Request]]:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, request=request, json={"success": True})

    http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return (
        DomofonLetaiClient(PHONE, access_token=TOKEN, api_client=http),
        http,
        requests,
    )


def cancel_message(invite: SipMessage) -> SipMessage:
    uri = invite.start_line.split()[1]
    return SipMessage(
        f"CANCEL {uri} SIP/2.0",
        (
            ("Via", invite.get_header("Via") or ""),
            ("From", invite.get_header("From") or ""),
            ("To", invite.get_header("To") or ""),
            ("Call-ID", invite.call_id or ""),
            ("CSeq", f"{invite.cseq_number} CANCEL"),
            ("Content-Length", "0"),
        ),
    )


def remote_bye(invite: SipMessage, to_value: str, *, cseq: int = 103) -> SipMessage:
    uri = invite.start_line.split()[1]
    return SipMessage(
        f"BYE {uri} SIP/2.0",
        (
            (
                "Via",
                f"SIP/2.0/TLS 192.0.2.10:9741;branch=z9hG4bK-bye-{cseq}",
            ),
            ("From", invite.get_header("From") or ""),
            ("To", to_value),
            ("Call-ID", invite.call_id or ""),
            ("CSeq", f"{cseq} BYE"),
            ("Content-Length", "0"),
        ),
    )


def response_for(
    request: SipMessage,
    status: int,
    reason: str,
    *,
    via: str | None = None,
) -> SipMessage:
    return SipMessage(
        f"SIP/2.0 {status} {reason}",
        (
            ("Via", via or request.get_header("Via") or ""),
            ("From", request.get_header("From") or ""),
            ("To", request.get_header("To") or ""),
            ("Call-ID", request.call_id or ""),
            ("CSeq", request.get_header("CSeq") or ""),
            ("Content-Length", "0"),
        ),
    )


async def wait_for_sent(
    connection: FakeSipConnection,
    predicate: Any,
    *,
    timeout: float = 1.0,
) -> SipMessage:
    async def find() -> SipMessage:
        while True:
            for message in connection.sent:
                if predicate(message):
                    return message
            await asyncio.sleep(0)

    return await asyncio.wait_for(find(), timeout=timeout)


def test_stream_reader_frames_fragmented_message_and_body() -> None:
    raw = invite_message().to_bytes()

    async def scenario() -> None:
        reader = asyncio.StreamReader()
        framed = SipStreamReader(reader, timeout=1)
        for byte in raw:
            reader.feed_data(bytes((byte,)))
        parsed = await framed.read_message()
        assert parsed.start_line.startswith("INVITE ")
        assert parsed.call_id == CALL_ID
        assert parsed.body == INVITE_SDP

    run(scenario())


def test_stream_reader_rejects_conflicting_content_length() -> None:
    async def scenario() -> None:
        reader = asyncio.StreamReader()
        reader.feed_data(
            b"OPTIONS sip:test@example SIP/2.0\r\n"
            b"Content-Length: 0\r\n"
            b"l: 1\r\n\r\n"
        )
        with pytest.raises(SipProtocolError, match="Conflicting"):
            await SipStreamReader(reader, timeout=1).read_message()

    run(scenario())


def test_register_digest_and_response_builders() -> None:
    register = RegisterBuilder(
        "D000000",
        "sip-secret",
        HOST,
        9741,
        "192.0.2.20",
        50000,
    )
    initial = register.build_initial()
    challenge = SipMessage(
        "SIP/2.0 401 Unauthorized",
        (
            ("Via", initial.get_header("Via") or ""),
            ("From", initial.get_header("From") or ""),
            ("To", initial.get_header("To") or ""),
            ("Call-ID", register.call_id),
            ("CSeq", initial.get_header("CSeq") or ""),
            (
                "WWW-Authenticate",
                'Digest realm="test", nonce="nonce", algorithm=MD5',
            ),
            ("Content-Length", "0"),
        ),
    )
    authenticated = register.build_authenticated(challenge)
    authorization = authenticated.get_header("Authorization") or ""

    assert authenticated.start_line == f"REGISTER sip:{HOST}:9741;transport=tls SIP/2.0"
    assert "transport=tls" in (authenticated.get_header("Contact") or "")
    assert 'username="D000000"' in authorization
    assert "sip-secret" not in authorization
    assert authenticated.cseq_number == (initial.cseq_number or 0) + 1

    responses = SipResponseBuilder(invite_message(), local_tag="local-tag")
    decline = responses.decline()
    assert decline.status_code == 603
    assert decline.get_headers("Via") == invite_message().get_headers("Via")
    assert "tag=local-tag" in (decline.get_header("To") or "")


def test_inactive_sdp_rejects_every_media_stream() -> None:
    answer = build_inactive_sdp_answer(INVITE_SDP).decode()
    assert "m=audio 0 RTP/AVP 8" in answer
    assert "m=video 0 RTP/AVP 99" in answer
    assert answer.count("a=inactive") == 2


def test_connect_register_decline_and_open_door() -> None:
    connection = FakeSipConnection()
    factory = FakeConnectionFactory(connection)

    async def scenario() -> None:
        client, http, requests = api_client()
        call = await connect_sip_call(
            client,
            incoming_event(),
            sip_settings(),
            _connection_factory=factory,
        )
        assert call.state is SipCallState.RINGING
        await call.open_door_and_end(42)
        assert call.state is SipCallState.ENDED
        assert any(message.status_code == 603 for message in connection.sent)
        assert requests[-1].url.path == "/v2/gate/open-door"
        assert json.loads(requests[-1].content)["gate_id"] == 42
        await call.aclose()
        await client.aclose()
        await http.aclose()

    run(scenario())
    assert factory.calls == [(HOST, 9741)]
    assert connection.closed


def test_answer_inactive_ack_and_hangup() -> None:
    connection = FakeSipConnection()
    factory = FakeConnectionFactory(connection)

    async def scenario() -> None:
        client, http, _requests = api_client()
        call = await connect_sip_call(
            client,
            incoming_event(),
            sip_settings(),
            _connection_factory=factory,
        )
        await call.answer_inactive()
        assert call.state is SipCallState.ESTABLISHED
        assert call.acknowledged

        answer = next(
            message
            for message in connection.sent
            if message.status_code == 200 and message.cseq_method == "INVITE"
        )
        assert b"m=audio 0" in answer.body
        assert b"m=video 0" in answer.body

        await call.hangup()
        assert call.state is SipCallState.ENDED
        assert any(message.method == "BYE" for message in connection.sent)
        await call.aclose()
        await client.aclose()
        await http.aclose()

    run(scenario())


def test_endpoint_and_call_correlation_are_strict_by_default() -> None:
    async def scenario() -> None:
        client, http, _requests = api_client()
        with pytest.raises(SipMetadataError, match="does not match"):
            await connect_sip_call(
                client,
                incoming_event(),
                sip_settings(address="other.example"),
                _connection_factory=FakeConnectionFactory(FakeSipConnection()),
            )

        mismatch = FakeSipConnection(invite_message(call_id="different"))
        with pytest.raises(SipTimeoutError):
            await connect_sip_call(
                client,
                incoming_event(),
                sip_settings(),
                connect_timeout=0.05,
                invite_timeout=0.01,
                _connection_factory=FakeConnectionFactory(mismatch),
            )
        assert mismatch.closed
        await client.aclose()
        await http.aclose()

    run(scenario())


def test_cancel_wins_before_answer_and_reserves_single_final_response() -> None:
    connection = FakeSipConnection(auto_ack=False)

    async def scenario() -> None:
        client, http, _requests = api_client()
        call = await connect_sip_call(
            client,
            incoming_event(),
            sip_settings(),
            transaction_timeout=0.2,
            t1=0.001,
            _connection_factory=FakeConnectionFactory(connection),
        )
        await connection.responses.put(cancel_message(connection.invite))
        cancel_ok = await wait_for_sent(
            connection,
            lambda message: (
                message.status_code == 200 and message.cseq_method == "CANCEL"
            ),
        )
        terminated = await wait_for_sent(
            connection,
            lambda message: (
                message.status_code == 487 and message.cseq_method == "INVITE"
            ),
        )
        assert connection.sent.index(cancel_ok) < connection.sent.index(terminated)

        with pytest.raises(SipCallStateError):
            await call.answer_inactive()
        final_invite_statuses = [
            message.status_code
            for message in connection.sent
            if message.cseq_method == "INVITE" and (message.status_code or 0) >= 200
        ]
        assert final_invite_statuses == [487]

        await call.wait_ended()
        await call.aclose()
        await client.aclose()
        await http.aclose()

    run(scenario())


def test_duplicate_incoming_bye_replays_cached_ok() -> None:
    connection = FakeSipConnection()

    async def scenario() -> None:
        client, http, _requests = api_client()
        call = await connect_sip_call(
            client,
            incoming_event(),
            sip_settings(),
            _connection_factory=FakeConnectionFactory(connection),
        )
        await call.answer_inactive()
        answer = next(
            message
            for message in connection.sent
            if message.status_code == 200 and message.cseq_method == "INVITE"
        )
        bye = remote_bye(connection.invite, answer.get_header("To") or "")
        await connection.responses.put(bye)
        await connection.responses.put(bye)

        async def two_ok_responses() -> bool:
            while True:
                responses = [
                    message
                    for message in connection.sent
                    if message.status_code == 200 and message.cseq_method == "BYE"
                ]
                if len(responses) == 2:
                    return True
                await asyncio.sleep(0)

        await asyncio.wait_for(two_ok_responses(), timeout=1)
        assert call.state is SipCallState.ENDED
        await call.aclose()
        await client.aclose()
        await http.aclose()

    run(scenario())


def test_malformed_bye_cseq_method_is_rejected() -> None:
    connection = FakeSipConnection()

    async def scenario() -> None:
        client, http, _requests = api_client()
        call = await connect_sip_call(
            client,
            incoming_event(),
            sip_settings(),
            _connection_factory=FakeConnectionFactory(connection),
        )
        await call.answer_inactive()
        answer = next(
            message
            for message in connection.sent
            if message.status_code == 200 and message.cseq_method == "INVITE"
        )
        valid = remote_bye(connection.invite, answer.get_header("To") or "")
        malformed = SipMessage(
            valid.start_line,
            tuple(
                (name, "103 OPTIONS")
                if name == "CSeq"
                else (name, "<sip:D000000@192.0.2.20>;tag=foreign")
                if name == "To"
                else (name, value)
                for name, value in valid.headers
            ),
        )
        await connection.responses.put(malformed)
        rejected = await wait_for_sent(
            connection,
            lambda message: message.status_code == 400,
        )
        assert rejected.cseq_method == "OPTIONS"
        assert call.state is SipCallState.ESTABLISHED
        await call.hangup()
        await call.aclose()
        await client.aclose()
        await http.aclose()

    run(scenario())


def test_local_bye_ignores_provisional_and_wrong_transaction_response() -> None:
    connection = FakeSipConnection(auto_bye_response=False)

    async def scenario() -> None:
        client, http, _requests = api_client()
        call = await connect_sip_call(
            client,
            incoming_event(),
            sip_settings(),
            transaction_timeout=0.2,
            _connection_factory=FakeConnectionFactory(connection),
        )
        await call.answer_inactive()
        hangup = asyncio.create_task(call.hangup())
        bye = await wait_for_sent(
            connection,
            lambda message: message.method == "BYE",
        )
        await connection.responses.put(response_for(bye, 180, "Ringing"))
        await asyncio.sleep(0)
        assert not hangup.done()

        original_via = bye.get_header("Via") or ""
        wrong_via = original_via.replace(
            "192.0.2.20:50000",
            "other.example:50000",
        )
        await connection.responses.put(
            response_for(bye, 200, "OK", via=wrong_via)
        )
        await asyncio.sleep(0)
        assert not hangup.done()

        await connection.responses.put(response_for(bye, 200, "OK"))
        await hangup
        assert call.state is SipCallState.ENDED
        await call.aclose()
        await client.aclose()
        await http.aclose()

    run(scenario())


def test_timer_l_retransmits_answer_and_sends_best_effort_bye() -> None:
    connection = FakeSipConnection(auto_ack=False)

    async def scenario() -> None:
        client, http, _requests = api_client()
        call = await connect_sip_call(
            client,
            incoming_event(),
            sip_settings(),
            transaction_timeout=0.05,
            t1=0.001,
            _connection_factory=FakeConnectionFactory(connection),
        )
        with pytest.raises(SipTimeoutError, match="ACK"):
            await call.answer_inactive()
        answers = [
            message
            for message in connection.sent
            if message.status_code == 200 and message.cseq_method == "INVITE"
        ]
        assert len(answers) >= 2
        assert any(message.method == "BYE" for message in connection.sent)
        assert call.state is SipCallState.FAILED
        await call.aclose()
        await client.aclose()
        await http.aclose()

    run(scenario())


def test_timer_h_keeps_non_2xx_transaction_until_expiry() -> None:
    connection = FakeSipConnection(auto_ack=False)

    async def scenario() -> None:
        client, http, _requests = api_client()
        call = await connect_sip_call(
            client,
            incoming_event(),
            sip_settings(),
            transaction_timeout=0.2,
            t1=0.001,
            _connection_factory=FakeConnectionFactory(connection),
        )
        decline = asyncio.create_task(call.decline())
        await wait_for_sent(
            connection,
            lambda message: message.status_code == 603,
        )
        await asyncio.sleep(0.01)
        assert not decline.done()
        await decline
        assert call.state is SipCallState.ENDED
        await call.aclose()
        await client.aclose()
        await http.aclose()

    run(scenario())


def test_deregister_retries_one_fresh_digest_challenge() -> None:
    connection = FakeSipConnection(challenge_deregister=True)

    async def scenario() -> None:
        client, http, _requests = api_client()
        call = await connect_sip_call(
            client,
            incoming_event(),
            sip_settings(),
            transaction_timeout=0.2,
            _connection_factory=FakeConnectionFactory(connection),
        )
        await call.decline()
        await call.aclose()
        deregisters = [
            message
            for message in connection.sent
            if message.method == "REGISTER" and message.get_header("Expires") == "0"
        ]
        assert len(deregisters) == 2
        assert 'nonce="fresh"' in (
            deregisters[-1].get_header("Authorization") or ""
        )
        await client.aclose()
        await http.aclose()

    run(scenario())


def test_client_close_during_connect_closes_transport(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = FakeSipConnection()
    entered = asyncio.Event()
    release = asyncio.Event()

    class ClientWithSipSettings(DomofonLetaiClient):
        async def get_sip_settings(self) -> SipSettings:
            return sip_settings()

    async def gated_factory(
        _host: str,
        _port: int,
        _context: ssl.SSLContext,
        _timeout: float | None,
    ) -> SipConnection:
        entered.set()
        await release.wait()
        return connection

    async def scenario() -> None:
        http = httpx.AsyncClient(
            transport=httpx.MockTransport(lambda _: httpx.Response(200))
        )
        client = ClientWithSipSettings(PHONE, access_token=TOKEN, api_client=http)
        monkeypatch.setattr("domofon_letai.calls._open_tls_connection", gated_factory)

        connecting = asyncio.create_task(client.connect_incoming_call(incoming_event()))
        await entered.wait()
        closing = asyncio.create_task(client.aclose())
        await asyncio.sleep(0)
        release.set()

        with pytest.raises(RuntimeError, match="closing or closed"):
            await connecting
        await closing
        assert connection.closed
        await http.aclose()

    run(scenario())


def test_cancellation_during_door_request_still_ends_sip_call() -> None:
    connection = FakeSipConnection()
    door_started = asyncio.Event()

    class BlockingDoorClient(DomofonLetaiClient):
        async def open_door(self, intercom_id: int, *, screen_id: int = 1) -> None:
            del intercom_id, screen_id
            door_started.set()
            await asyncio.Event().wait()

    async def scenario() -> None:
        http = httpx.AsyncClient(
            transport=httpx.MockTransport(lambda _: httpx.Response(200))
        )
        client = BlockingDoorClient(PHONE, access_token=TOKEN, api_client=http)
        call = await connect_sip_call(
            client,
            incoming_event(),
            sip_settings(),
            _connection_factory=FakeConnectionFactory(connection),
        )
        operation = asyncio.create_task(call.open_door_and_end(42))
        await door_started.wait()
        operation.cancel()
        with pytest.raises(asyncio.CancelledError):
            await operation
        assert any(message.status_code == 603 for message in connection.sent)
        await call.aclose()
        await client.aclose()
        await http.aclose()

    run(scenario())


def test_cancel_buffered_before_register_ok_is_not_lost() -> None:
    connection = FakeSipConnection(early_invite_cancel=True)

    async def scenario() -> None:
        client, http, _requests = api_client()
        call = await connect_sip_call(
            client,
            incoming_event(),
            sip_settings(),
            t1=0.001,
            _connection_factory=FakeConnectionFactory(connection),
        )
        assert any(
            message.status_code == 200 and message.cseq_method == "CANCEL"
            for message in connection.sent
        )
        assert any(
            message.status_code == 487 and message.cseq_method == "INVITE"
            for message in connection.sent
        )
        with pytest.raises(SipCallStateError):
            await call.answer_inactive()
        await call.wait_ended()
        await call.aclose()
        await client.aclose()
        await http.aclose()

    run(scenario())


def test_wrong_dialog_bye_gets_481_without_failing_active_call() -> None:
    connection = FakeSipConnection()

    async def scenario() -> None:
        client, http, _requests = api_client()
        call = await connect_sip_call(
            client,
            incoming_event(),
            sip_settings(),
            _connection_factory=FakeConnectionFactory(connection),
        )
        await call.answer_inactive()
        answer = next(
            message
            for message in connection.sent
            if message.status_code == 200 and message.cseq_method == "INVITE"
        )
        bye = remote_bye(connection.invite, answer.get_header("To") or "")
        wrong_dialog = SipMessage(
            bye.start_line,
            tuple(
                (name, "<sip:D000000@192.0.2.20>;tag=wrong")
                if name == "To"
                else (name, value)
                for name, value in bye.headers
            ),
        )
        await connection.responses.put(wrong_dialog)
        await wait_for_sent(
            connection,
            lambda message: (
                message.status_code == 481 and message.cseq_method == "BYE"
            ),
        )
        assert call.state is SipCallState.ESTABLISHED
        assert call.last_error is None
        await call.hangup()
        await call.aclose()
        await client.aclose()
        await http.aclose()

    run(scenario())


def test_ack_timeout_is_preserved_when_cleanup_bye_send_fails() -> None:
    connection = FakeSipConnection(
        auto_ack=False,
        fail_bye_send=True,
    )

    async def scenario() -> None:
        client, http, _requests = api_client()
        call = await connect_sip_call(
            client,
            incoming_event(),
            sip_settings(),
            transaction_timeout=0.05,
            t1=0.001,
            _connection_factory=FakeConnectionFactory(connection),
        )
        with pytest.raises(SipTimeoutError, match="ACK"):
            await call.answer_inactive()
        assert call.state is SipCallState.FAILED
        assert isinstance(call.last_error, SipTimeoutError)
        assert any(message.method == "BYE" for message in connection.sent)
        await call.aclose()
        await client.aclose()
        await http.aclose()

    run(scenario())
