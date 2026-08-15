from __future__ import annotations

import asyncio
import json
import os
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

import httpx
import pytest

from domofon_letai import (
    CredentialStoreError,
    DomofonLetaiClient,
    FileFcmCredentialStore,
    IncomingCallListener,
    IncomingCallListenerState,
    PushError,
)
from domofon_letai._fcm import _FirebaseBackend
from domofon_letai.push import CredentialCallback, NotificationCallback

PHONE = "79000000000"
TOKEN = "access-token"
FCM_TOKEN = "fcm-token"


class MemoryStore:
    def __init__(self, credentials: dict[str, Any] | None = None) -> None:
        self.credentials = credentials
        self.saved: list[dict[str, Any]] = []

    async def load(self) -> dict[str, Any] | None:
        return self.credentials

    async def save(self, credentials: Mapping[str, Any]) -> None:
        self.credentials = dict(credentials)
        self.saved.append(dict(credentials))


class FakeBackend:
    def __init__(
        self,
        notification_callback: NotificationCallback,
        credential_callback: CredentialCallback,
        credentials: dict[str, Any] | None,
    ) -> None:
        self.notification_callback = notification_callback
        self.credential_callback = credential_callback
        self.credentials = credentials
        self.started = False
        self.stopped = False
        self.failure = asyncio.Event()
        self.failure_error: BaseException = PushError("backend failed")

    async def checkin_or_register(self) -> str:
        self.credential_callback(_credentials(FCM_TOKEN))
        return FCM_TOKEN

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.stopped = True

    async def wait_for_failure(self) -> None:
        await self.failure.wait()
        raise self.failure_error

    def fail(self, error: BaseException | None = None) -> None:
        if error is not None:
            self.failure_error = error
        self.failure.set()

    def emit(
        self,
        data: dict[str, Any],
        message_id: str = "persistent-1",
    ) -> None:
        self.notification_callback({"data": data}, message_id, None)

    def rotate(self, token: str) -> None:
        self.credential_callback(_credentials(token))


class SlowStopBackend(FakeBackend):
    def __init__(
        self,
        notification_callback: NotificationCallback,
        credential_callback: CredentialCallback,
        credentials: dict[str, Any] | None,
    ) -> None:
        super().__init__(notification_callback, credential_callback, credentials)
        self.stop_started = asyncio.Event()
        self.stop_release = asyncio.Event()

    async def stop(self) -> None:
        self.stop_started.set()
        await self.stop_release.wait()
        self.stopped = True


class BackendFactory:
    backend_class = FakeBackend

    def __init__(self) -> None:
        self.backend: FakeBackend | None = None

    def __call__(
        self,
        notification_callback: NotificationCallback,
        credential_callback: CredentialCallback,
        credentials: dict[str, Any] | None,
    ) -> FakeBackend:
        self.backend = self.backend_class(
            notification_callback,
            credential_callback,
            credentials,
        )
        return self.backend


class SlowBackendFactory(BackendFactory):
    backend_class = SlowStopBackend


def _credentials(token: str) -> dict[str, Any]:
    return {"fcm": {"registration": {"token": token}}, "device": {"id": "1"}}


def _call_payload() -> dict[str, Any]:
    return {
        "category": "start_call",
        "title": "intercom_sip_login=G17126",
        "body": "startcall",
        "uuid": "srid-6a4cc94f",
        "sip_address": "dmf-proxy01.tattelecom.ru",
        "sip_port": "9741",
        "sip_transport": "tls",
        "sip_call_id": "dom2-aster-01-1785514214.1189358",
    }


def _client(
    handler: Callable[[httpx.Request], httpx.Response],
) -> tuple[DomofonLetaiClient, httpx.AsyncClient]:
    api_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    client = DomofonLetaiClient(
        PHONE,
        access_token=TOKEN,
        api_client=api_client,
    )
    return client, api_client


def run(coroutine):
    return asyncio.run(coroutine)


def test_incoming_call_listener_contract_and_lifecycle() -> None:
    requests: list[httpx.Request] = []
    factory = BackendFactory()
    store = MemoryStore()

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, request=request, json={"success": True})

    async def scenario() -> None:
        client, api_client = _client(handler)
        listener = IncomingCallListener(
            client,
            store,
            _backend_factory=factory,
        )

        async with listener:
            assert listener.state is IncomingCallListenerState.RUNNING
            assert factory.backend is not None
            factory.backend.emit(_call_payload())
            event = await asyncio.wait_for(anext(listener), 1)

            assert event.message_id == "persistent-1"
            assert event.sip_login == "G17126"
            assert event.call_id == "dom2-aster-01-1785514214.1189358"
            assert event.sip_address == "dmf-proxy01.tattelecom.ru"
            assert event.sip_port == 9741
            assert event.sip_transport == "tls"
            assert event.received_at.tzinfo is not None
            with pytest.raises(TypeError):
                event.raw_data["category"] = "changed"  # type: ignore[index]

        assert listener.state is IncomingCallListenerState.CLOSED
        assert factory.backend.stopped
        assert store.saved
        await client.aclose()
        await api_client.aclose()

    run(scenario())

    assert requests[0].url.path == "/v2/subscriber/update-push-token"
    assert requests[0].headers["access-token"] == TOKEN
    assert json.loads(requests[0].content) == {
        "push_service": "fcm",
        "push_token": FCM_TOKEN,
    }


def test_unknown_and_duplicate_notifications_are_ignored() -> None:
    factory = BackendFactory()

    async def scenario() -> None:
        client, api_client = _client(
            lambda request: httpx.Response(
                200, request=request, json={"success": True}
            )
        )
        listener = IncomingCallListener(
            client,
            MemoryStore(),
            _backend_factory=factory,
        )
        await listener.start()
        assert factory.backend is not None

        factory.backend.emit({"category": "message"}, "ignored")
        factory.backend.emit(_call_payload(), "same-id")
        factory.backend.emit(_call_payload(), "same-id")
        first = await asyncio.wait_for(anext(listener), 1)
        assert first.message_id == "same-id"

        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(anext(listener), 0.01)

        await listener.aclose()
        await client.aclose()
        await api_client.aclose()

    run(scenario())


def test_token_rotation_is_saved_and_registered() -> None:
    requests: list[httpx.Request] = []
    factory = BackendFactory()
    store = MemoryStore()

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, request=request, json={"success": True})

    async def scenario() -> None:
        client, api_client = _client(handler)
        listener = IncomingCallListener(
            client,
            store,
            _backend_factory=factory,
        )
        await listener.start()
        assert factory.backend is not None
        factory.backend.rotate("stale-token")
        factory.backend.rotate("rotated-token")
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        await listener.aclose()
        await client.aclose()
        await api_client.aclose()

    run(scenario())
    assert len(requests) == 2
    assert json.loads(requests[-1].content)["push_token"] == "rotated-token"
    assert store.credentials == _credentials("rotated-token")


def test_stale_credential_failure_does_not_override_fresh_token() -> None:
    factory = BackendFactory()
    stale_started = asyncio.Event()
    release_stale = asyncio.Event()
    requested_tokens: list[str] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        token = str(json.loads(request.content)["push_token"])
        requested_tokens.append(token)
        if token == "stale-token":
            stale_started.set()
            await release_stale.wait()
            return httpx.Response(
                500,
                request=request,
                json={"success": False, "error_text": "stale failed"},
            )
        return httpx.Response(200, request=request, json={"success": True})

    async def scenario() -> None:
        api_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        client = DomofonLetaiClient(
            PHONE,
            access_token=TOKEN,
            api_client=api_client,
        )
        listener = IncomingCallListener(
            client,
            MemoryStore(),
            _backend_factory=factory,
        )
        await listener.start()
        assert factory.backend is not None

        factory.backend.rotate("stale-token")
        await stale_started.wait()
        factory.backend.rotate("fresh-token")
        release_stale.set()

        for _ in range(20):
            if requested_tokens[-1:] == ["fresh-token"]:
                break
            await asyncio.sleep(0)

        assert listener.state is IncomingCallListenerState.RUNNING
        assert listener.last_error is None
        assert requested_tokens == [FCM_TOKEN, "stale-token", "fresh-token"]
        await listener.aclose()
        await client.aclose()
        await api_client.aclose()

    run(scenario())


def test_queue_overflow_is_reported() -> None:
    factory = BackendFactory()

    async def scenario() -> None:
        client, api_client = _client(
            lambda request: httpx.Response(
                200, request=request, json={"success": True}
            )
        )
        listener = IncomingCallListener(
            client,
            MemoryStore(),
            max_pending_events=1,
            _backend_factory=factory,
        )
        await listener.start()
        assert factory.backend is not None
        factory.backend.emit(_call_payload(), "one")
        factory.backend.emit(_call_payload(), "two")
        await asyncio.sleep(0)

        assert listener.state is IncomingCallListenerState.FAILED
        with pytest.raises(PushError, match="overflow"):
            await anext(listener)

        await listener.aclose()
        await client.aclose()
        await api_client.aclose()

    run(scenario())


def test_background_backend_failure_is_terminal() -> None:
    factory = BackendFactory()

    async def scenario() -> None:
        client, api_client = _client(
            lambda request: httpx.Response(
                200, request=request, json={"success": True}
            )
        )
        listener = IncomingCallListener(
            client,
            MemoryStore(),
            _backend_factory=factory,
        )
        await listener.start()
        assert factory.backend is not None
        factory.backend.fail(PushError("connection lost"))
        await asyncio.sleep(0)
        await asyncio.sleep(0)

        assert listener.state is IncomingCallListenerState.FAILED
        with pytest.raises(PushError, match="connection lost"):
            await anext(listener)
        with pytest.raises(PushError, match="connection lost"):
            await anext(listener)

        await listener.aclose()
        await client.aclose()
        await api_client.aclose()

    run(scenario())


def test_client_closes_created_listener() -> None:
    factory = BackendFactory()

    async def scenario() -> None:
        client, api_client = _client(
            lambda request: httpx.Response(
                200, request=request, json={"success": True}
            )
        )
        listener = IncomingCallListener(
            client,
            MemoryStore(),
            _backend_factory=factory,
        )
        await listener.start()
        await client.aclose()
        assert listener.state is IncomingCallListenerState.CLOSED
        await api_client.aclose()

    run(scenario())


def test_client_close_waits_for_backend_and_rejects_new_listener() -> None:
    factory = SlowBackendFactory()

    async def scenario() -> None:
        client, api_client = _client(
            lambda request: httpx.Response(
                200, request=request, json={"success": True}
            )
        )
        listener = IncomingCallListener(
            client,
            MemoryStore(),
            _backend_factory=factory,
        )
        await listener.start()
        assert isinstance(factory.backend, SlowStopBackend)

        close_task = asyncio.create_task(client.aclose())
        await factory.backend.stop_started.wait()
        assert not close_task.done()
        with pytest.raises(RuntimeError, match="closing"):
            client.incoming_calls(credential_store=MemoryStore())
        with pytest.raises(RuntimeError, match="closing"):
            await client.list_intercoms()

        close_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await close_task

        factory.backend.stop_release.set()
        await client.aclose()
        assert factory.backend.stopped
        assert listener.state is IncomingCallListenerState.CLOSED
        await api_client.aclose()

    run(scenario())


def test_firebase_backend_tracks_all_tasks_and_prestart_stop_is_safe() -> None:
    class RawFirebaseClient:
        def __init__(self) -> None:
            self.run_state = "created"
            self.tasks: list[asyncio.Task[None]] = []
            self.stop_calls = 0
            self.never = asyncio.Event()

        async def start(self) -> None:
            self.run_state = "started"

            async def fail_monitor() -> None:
                await asyncio.sleep(0)
                raise RuntimeError("monitor failed")

            self.tasks = [
                asyncio.create_task(self.never.wait()),
                asyncio.create_task(fail_monitor()),
            ]

        async def stop(self) -> None:
            self.stop_calls += 1
            for task in self.tasks:
                task.cancel()

    async def scenario() -> None:
        raw = RawFirebaseClient()
        backend = _FirebaseBackend(raw, "started")
        await backend.stop()
        assert raw.stop_calls == 0

        await backend.start()
        with pytest.raises(PushError, match="RuntimeError"):
            await backend.wait_for_failure()
        await backend.stop()
        assert raw.stop_calls == 1

    run(scenario())


def test_get_sip_settings() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200,
            request=request,
            json={
                "success": True,
                "sip_address": "dmf-proxy01.tattelecom.ru",
                "sip_port": 9740,
                "sip_login": "D000000",
                "sip_password": "secret",
                "reg_expire_time": 60,
            },
        )

    async def scenario() -> None:
        client, api_client = _client(handler)
        settings = await client.get_sip_settings()
        assert settings.address == "dmf-proxy01.tattelecom.ru"
        assert settings.port == 9740
        assert settings.login == "D000000"
        assert settings.password == "secret"
        assert settings.registration_expires == 60
        assert "secret" not in repr(settings)
        await client.aclose()
        await api_client.aclose()

    run(scenario())
    assert requests[0].url.path == "/v1/subscriber/sipsettings"
    assert requests[0].url.params["device_code"] == "Android_empty_push_token"
    assert requests[0].url.params["phone"] == PHONE


def test_file_credential_store_round_trip(tmp_path: Path) -> None:
    path = tmp_path / "state" / "fcm.json"
    store = FileFcmCredentialStore(path)

    async def scenario() -> None:
        assert await store.load() is None
        await store.save(_credentials(FCM_TOKEN))
        assert await store.load() == _credentials(FCM_TOKEN)

    run(scenario())
    if os.name == "posix":
        assert path.stat().st_mode & 0o777 == 0o600


def test_file_credential_store_rejects_corruption(tmp_path: Path) -> None:
    path = tmp_path / "fcm.json"
    path.write_text("not-json", encoding="utf-8")
    store = FileFcmCredentialStore(path)

    async def scenario() -> None:
        with pytest.raises(CredentialStoreError):
            await store.load()

    run(scenario())
