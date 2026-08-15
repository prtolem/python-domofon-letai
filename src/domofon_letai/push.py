"""Incoming-call notifications and credential persistence."""

from __future__ import annotations

import asyncio
import json
import os
import tempfile
from collections import deque
from collections.abc import AsyncIterator, Callable, Mapping
from contextlib import suppress
from copy import deepcopy
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Protocol

from ._constants import PUSH_CATEGORY_START_CALL
from .exceptions import (
    CredentialStoreError,
    DomofonLetaiError,
    PushError,
)
from .models import IncomingCallEvent

if TYPE_CHECKING:
    from .client import DomofonLetaiClient


class FcmCredentialStore(Protocol):
    """Persistence contract for opaque Firebase device credentials."""

    async def load(self) -> dict[str, Any] | None:
        """Load credentials saved by an earlier listener run."""
        ...

    async def save(self, credentials: Mapping[str, Any]) -> None:
        """Atomically replace the stored credentials."""
        ...


class FileFcmCredentialStore:
    """Store FCM credentials as a versioned JSON file with restricted permissions."""

    _VERSION = 1

    def __init__(self, path: str | Path) -> None:
        self._path = Path(path)
        self._lock = asyncio.Lock()

    @property
    def path(self) -> Path:
        """Credential file location."""

        return self._path

    async def load(self) -> dict[str, Any] | None:
        """Load and validate the credential envelope."""

        async with self._lock:
            return await asyncio.to_thread(self._load_sync)

    async def save(self, credentials: Mapping[str, Any]) -> None:
        """Persist credentials using a same-directory atomic replacement."""

        snapshot = deepcopy(dict(credentials))
        async with self._lock:
            await asyncio.to_thread(self._save_sync, snapshot)

    def _load_sync(self) -> dict[str, Any] | None:
        if not self._path.exists():
            return None

        try:
            with self._path.open(encoding="utf-8") as source:
                envelope = json.load(source)
        except (OSError, ValueError, TypeError) as error:
            raise CredentialStoreError("failed to read FCM credential store") from error

        if not isinstance(envelope, dict) or envelope.get("version") != self._VERSION:
            raise CredentialStoreError("unsupported FCM credential store format")

        credentials = envelope.get("credentials")
        if not isinstance(credentials, dict):
            raise CredentialStoreError("FCM credential store has no credentials object")

        return credentials

    def _save_sync(self, credentials: dict[str, Any]) -> None:
        parent_existed = self._path.parent.exists()
        temporary_path: Path | None = None

        try:
            self._path.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
            if not parent_existed:
                os.chmod(self._path.parent, 0o700)

            with tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                dir=self._path.parent,
                prefix=f".{self._path.name}.",
                delete=False,
            ) as temporary:
                temporary_path = Path(temporary.name)
                os.chmod(temporary_path, 0o600)
                json.dump(
                    {"version": self._VERSION, "credentials": credentials},
                    temporary,
                    ensure_ascii=False,
                    separators=(",", ":"),
                )
                temporary.flush()
                os.fsync(temporary.fileno())

            os.replace(temporary_path, self._path)
            os.chmod(self._path, 0o600)
        except (OSError, TypeError, ValueError) as error:
            if temporary_path is not None:
                with suppress(OSError):
                    temporary_path.unlink()
            raise CredentialStoreError("failed to save FCM credential store") from error


class IncomingCallListenerState(str, Enum):
    """Lifecycle state of an incoming-call listener."""

    NEW = "new"
    STARTING = "starting"
    RUNNING = "running"
    FAILED = "failed"
    CLOSING = "closing"
    CLOSED = "closed"


class FcmBackend(Protocol):
    """Private adapter boundary around the optional Firebase dependency."""

    async def checkin_or_register(self) -> str:
        """Return a usable FCM registration token."""
        ...

    async def start(self) -> None:
        """Start receiving messages."""
        ...

    async def stop(self) -> None:
        """Stop receiving messages and release resources."""
        ...

    async def wait_for_failure(self) -> None:
        """Block until the backend terminates unexpectedly."""
        ...


NotificationCallback = Callable[[dict[str, Any], str, Any], None]
CredentialCallback = Callable[[dict[str, Any]], None]
BackendFactory = Callable[
    [NotificationCallback, CredentialCallback, dict[str, Any] | None],
    FcmBackend,
]

_STOP = object()


class IncomingCallListener(AsyncIterator[IncomingCallEvent]):
    """Single-consumer async iterator of incoming-call announcements."""

    def __init__(
        self,
        client: DomofonLetaiClient,
        credential_store: FcmCredentialStore,
        *,
        max_pending_events: int = 32,
        _backend_factory: BackendFactory | None = None,
    ) -> None:
        if max_pending_events < 1:
            raise ValueError("max_pending_events must be at least 1")

        self._client = client
        self._credential_store = credential_store
        self._backend_factory = _backend_factory or _create_default_backend
        self._queue: asyncio.Queue[IncomingCallEvent | BaseException | object] = (
            asyncio.Queue(max_pending_events)
        )
        self._lock = asyncio.Lock()
        self._state = IncomingCallListenerState.NEW
        self._last_error: BaseException | None = None
        self._backend: FcmBackend | None = None
        self._backend_monitor_task: asyncio.Task[None] | None = None
        self._shutdown_task: asyncio.Task[None] | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._current_token: str | None = None
        self._startup_credentials: list[dict[str, Any]] = []
        self._credential_generation = 0
        self._credential_update_lock = asyncio.Lock()
        self._background_tasks: set[asyncio.Task[None]] = set()
        self._seen_ids: deque[str] = deque(maxlen=128)
        self._seen_id_set: set[str] = set()
        self._client._register_incoming_call_listener(self)

    @property
    def state(self) -> IncomingCallListenerState:
        """Current listener lifecycle state."""

        return self._state

    @property
    def last_error(self) -> BaseException | None:
        """Last background failure, if any."""

        return self._last_error

    async def __aenter__(self) -> IncomingCallListener:
        await self.start()
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.aclose()

    def __aiter__(self) -> IncomingCallListener:
        return self

    async def __anext__(self) -> IncomingCallEvent:
        if self._state is IncomingCallListenerState.CLOSED and self._queue.empty():
            raise StopAsyncIteration
        if self._state is IncomingCallListenerState.FAILED and self._queue.empty():
            raise self._last_error or PushError("incoming-call listener failed")

        item = await self._queue.get()
        if item is _STOP:
            raise StopAsyncIteration
        if isinstance(item, BaseException):
            raise item
        if not isinstance(item, IncomingCallEvent):
            raise PushError("incoming-call queue contained an invalid item")
        return item

    async def start(self) -> None:
        """Register FCM credentials and start the message listener."""

        async with self._lock:
            if self._state is IncomingCallListenerState.RUNNING:
                return
            if self._state is not IncomingCallListenerState.NEW:
                raise PushError(f"listener cannot start from state {self._state.value}")

            self._state = IncomingCallListenerState.STARTING
            self._loop = asyncio.get_running_loop()

            try:
                credentials = await self._credential_store.load()
                self._backend = self._backend_factory(
                    self._on_notification,
                    self._on_credentials_updated,
                    credentials,
                )
                token = await self._backend.checkin_or_register()
                if not token:
                    raise PushError("Firebase did not return an FCM token")

                await asyncio.sleep(0)
                rotated_token = await self._flush_startup_credentials()
                token = rotated_token or token
                await self._client._register_fcm_token(token)
                self._current_token = token

                await self._backend.start()
                await asyncio.sleep(0)
                rotated_token = await self._flush_startup_credentials()
                if rotated_token and rotated_token != self._current_token:
                    await self._client._register_fcm_token(rotated_token)
                    self._current_token = rotated_token

                self._state = IncomingCallListenerState.RUNNING
                self._backend_monitor_task = asyncio.create_task(
                    self._monitor_backend()
                )
            except asyncio.CancelledError:
                with suppress(Exception):
                    await self._shutdown()
                self._state = IncomingCallListenerState.CLOSED
                self._client._discard_incoming_call_listener(self)
                raise
            except DomofonLetaiError as error:
                with suppress(Exception):
                    await self._shutdown()
                self._last_error = error
                self._state = IncomingCallListenerState.FAILED
                raise
            except Exception as error:
                with suppress(Exception):
                    await self._shutdown()
                wrapped = PushError(
                    f"failed to start incoming-call listener: {type(error).__name__}"
                )
                self._last_error = wrapped
                self._state = IncomingCallListenerState.FAILED
                raise wrapped from error

    async def aclose(self) -> None:
        """Stop receiving calls. The operation is idempotent."""

        async with self._lock:
            if self._state is IncomingCallListenerState.CLOSED:
                return

            self._state = IncomingCallListenerState.CLOSING
            shutdown_error: BaseException | None = None
            try:
                await self._shutdown()
            except Exception as error:
                shutdown_error = error
                self._last_error = error

            tasks = tuple(self._background_tasks)
            if tasks:
                results = await asyncio.gather(*tasks, return_exceptions=True)
                for result in results:
                    if isinstance(result, BaseException):
                        self._last_error = result

            self._state = IncomingCallListenerState.CLOSED
            self._client._discard_incoming_call_listener(self)
            self._replace_queue_with(_STOP)
            if shutdown_error is not None:
                message = "failed to stop incoming-call listener"
                raise PushError(message) from shutdown_error

    async def _shutdown(self) -> None:
        if self._shutdown_task is None:
            self._shutdown_task = asyncio.create_task(self._shutdown_backend())
        await asyncio.shield(self._shutdown_task)

    async def _shutdown_backend(self) -> None:
        monitor, self._backend_monitor_task = self._backend_monitor_task, None
        if monitor is not None and monitor is not asyncio.current_task():
            monitor.cancel()
            with suppress(asyncio.CancelledError):
                await monitor

        backend, self._backend = self._backend, None
        if backend is not None:
            await backend.stop()

    async def _monitor_backend(self) -> None:
        backend = self._backend
        if backend is None:
            return
        try:
            await backend.wait_for_failure()
            raise PushError("incoming-call backend stopped unexpectedly")
        except asyncio.CancelledError:
            raise
        except Exception as error:
            if self._state is IncomingCallListenerState.RUNNING:
                wrapped = (
                    error
                    if isinstance(error, DomofonLetaiError)
                    else PushError(
                        "incoming-call backend failed: "
                        f"{type(error).__name__}"
                    )
                )
                self._fail(wrapped)

    async def _flush_startup_credentials(self) -> str | None:
        if not self._startup_credentials:
            return None

        latest = self._startup_credentials[-1]
        self._startup_credentials.clear()
        await self._credential_store.save(latest)
        return _token_from_credentials(latest)

    def _on_credentials_updated(self, credentials: dict[str, Any]) -> None:
        snapshot = deepcopy(credentials)
        loop = self._loop
        if loop is not None and not loop.is_closed():
            loop.call_soon_threadsafe(self._accept_credentials_update, snapshot)

    def _accept_credentials_update(self, credentials: dict[str, Any]) -> None:
        if self._state is IncomingCallListenerState.STARTING:
            self._startup_credentials.append(credentials)
            return
        if self._state is not IncomingCallListenerState.RUNNING:
            return

        self._credential_generation += 1
        generation = self._credential_generation
        task = asyncio.create_task(
            self._persist_rotated_credentials(credentials, generation)
        )
        self._background_tasks.add(task)
        task.add_done_callback(self._background_task_done)

    async def _persist_rotated_credentials(
        self,
        credentials: dict[str, Any],
        generation: int,
    ) -> None:
        async with self._credential_update_lock:
            if generation < self._credential_generation:
                return

            try:
                await self._credential_store.save(credentials)
                if generation < self._credential_generation:
                    return

                token = _token_from_credentials(credentials)
                if token and token != self._current_token:
                    await self._client._register_fcm_token(token)
                    if generation == self._credential_generation:
                        self._current_token = token
            except Exception:
                if generation < self._credential_generation:
                    return
                raise

    def _background_task_done(self, task: asyncio.Task[None]) -> None:
        self._background_tasks.discard(task)
        if task.cancelled():
            return
        error = task.exception()
        if error is None:
            return
        self._last_error = error
        if self._state is IncomingCallListenerState.RUNNING:
            self._fail(error)

    def _on_notification(
        self,
        notification: dict[str, Any],
        persistent_id: str,
        _context: Any = None,
    ) -> None:
        data = notification.get("data")
        if not isinstance(data, dict):
            return

        snapshot = deepcopy(data)
        loop = self._loop
        if loop is not None and not loop.is_closed():
            loop.call_soon_threadsafe(
                self._accept_notification,
                snapshot,
                str(persistent_id),
            )

    def _accept_notification(self, data: dict[str, Any], message_id: str) -> None:
        if self._state is not IncomingCallListenerState.RUNNING:
            return
        if data.get("category") != PUSH_CATEGORY_START_CALL:
            return
        if message_id and message_id in self._seen_id_set:
            return

        event = _parse_incoming_call(data, message_id)
        try:
            self._queue.put_nowait(event)
        except asyncio.QueueFull:
            self._fail(PushError("incoming-call event queue overflow"))
            return

        if message_id:
            if len(self._seen_ids) == self._seen_ids.maxlen:
                removed = self._seen_ids.popleft()
                self._seen_id_set.discard(removed)
            self._seen_ids.append(message_id)
            self._seen_id_set.add(message_id)

    def _fail(self, error: BaseException) -> None:
        self._last_error = error
        self._state = IncomingCallListenerState.FAILED
        self._replace_queue_with(error)
        if self._backend is not None and self._shutdown_task is None:
            self._shutdown_task = asyncio.create_task(self._shutdown_backend())
            self._shutdown_task.add_done_callback(_consume_task_result)

    def _replace_queue_with(self, item: BaseException | object) -> None:
        while not self._queue.empty():
            with suppress(asyncio.QueueEmpty):
                self._queue.get_nowait()
        with suppress(asyncio.QueueFull):
            self._queue.put_nowait(item)


def _create_default_backend(
    notification_callback: NotificationCallback,
    credential_callback: CredentialCallback,
    credentials: dict[str, Any] | None,
) -> FcmBackend:
    from ._fcm import create_fcm_backend

    return create_fcm_backend(
        notification_callback,
        credential_callback,
        credentials,
    )


def _token_from_credentials(credentials: Mapping[str, Any]) -> str | None:
    fcm = credentials.get("fcm")
    if not isinstance(fcm, Mapping):
        return None
    registration = fcm.get("registration")
    if not isinstance(registration, Mapping):
        return None
    token = registration.get("token")
    return token if isinstance(token, str) and token else None


def _parse_incoming_call(data: dict[str, Any], message_id: str) -> IncomingCallEvent:
    title = str(data.get("title") or "")
    sip_login: str | None = None
    marker = "intercom_sip_login="
    if marker in title:
        sip_login = title.split(marker, 1)[1].strip() or None

    return IncomingCallEvent(
        received_at=datetime.now(timezone.utc),
        message_id=message_id,
        call_id=_optional_string(data.get("sip_call_id")),
        notification_uuid=_optional_string(data.get("uuid")),
        sip_login=sip_login,
        sip_address=_optional_string(data.get("sip_address")),
        sip_port=_optional_integer(data.get("sip_port")),
        sip_transport=_optional_string(data.get("sip_transport")),
        raw_data=MappingProxyType(dict(data)),
    )


def _optional_string(value: Any) -> str | None:
    return str(value) if value is not None and str(value) else None


def _optional_integer(value: Any) -> int | None:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _consume_task_result(task: asyncio.Task[None]) -> None:
    if not task.cancelled():
        with suppress(Exception):
            task.result()
