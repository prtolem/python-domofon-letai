"""Adapter for the optional ``firebase-messaging`` dependency."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from ._constants import (
    FIREBASE_API_KEY,
    FIREBASE_APP_ID,
    FIREBASE_PROJECT_ID,
    FIREBASE_SENDER_ID,
)
from .exceptions import PushDependencyError, PushError
from .push import CredentialCallback, FcmBackend, NotificationCallback

_LOGGER = logging.getLogger(__name__)
_HARDENED = False


class _FirebaseBackend:
    _START_TIMEOUT = 60.0

    def __init__(self, client: Any, started_state: Any) -> None:
        self._client = client
        self._started_state = started_state
        self._upstream_started = False
        self._tasks: tuple[asyncio.Task[Any], ...] = ()

    async def checkin_or_register(self) -> str:
        token = await self._client.checkin_or_register()
        return str(token)

    async def start(self) -> None:
        await self._client.start()
        self._upstream_started = True
        self._tasks = tuple(getattr(self._client, "tasks", ()))
        if not self._tasks:
            raise PushError("Firebase listener did not create background tasks")

        loop = asyncio.get_running_loop()
        deadline = loop.time() + self._START_TIMEOUT
        while self._client.run_state != self._started_state:
            if any(task.done() for task in self._tasks):
                raise PushError("Firebase task stopped before login completed")
            if loop.time() >= deadline:
                raise PushError("Firebase listener login timed out")
            await asyncio.sleep(0.05)

    async def stop(self) -> None:
        if not self._upstream_started:
            return
        await self._client.stop()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._upstream_started = False

    async def wait_for_failure(self) -> None:
        if not self._tasks:
            raise PushError("Firebase listener has no background tasks")

        done, _ = await asyncio.wait(
            self._tasks,
            return_when=asyncio.FIRST_COMPLETED,
        )
        failed_task = next(iter(done))
        if failed_task.cancelled():
            raise PushError("Firebase task was cancelled unexpectedly")
        error = failed_task.exception()
        if error is not None:
            raise PushError(
                f"Firebase task failed: {type(error).__name__}"
            ) from error
        raise PushError("Firebase task stopped unexpectedly")


def create_fcm_backend(
    notification_callback: NotificationCallback,
    credential_callback: CredentialCallback,
    credentials: dict[str, Any] | None,
) -> FcmBackend:
    """Create the real Firebase backend while keeping the dependency optional."""

    try:
        from firebase_messaging import (
            FcmPushClient,
            FcmPushClientRunState,
            FcmRegisterConfig,
        )
    except ImportError as error:
        raise PushDependencyError(
            'incoming calls require: pip install "domofon-letai-api[calls]"'
        ) from error

    _harden_fcm_client(FcmPushClient)
    client = FcmPushClient(
        notification_callback,
        FcmRegisterConfig(
            project_id=FIREBASE_PROJECT_ID,
            app_id=FIREBASE_APP_ID,
            api_key=FIREBASE_API_KEY,
            messaging_sender_id=FIREBASE_SENDER_ID,
        ),
        credentials,
        credential_callback,
    )
    return _FirebaseBackend(client, FcmPushClientRunState.STARTED)


def _harden_fcm_client(client_class: Any) -> None:
    """Keep malformed encrypted pushes from terminating the MCS listener."""

    global _HARDENED
    if _HARDENED:
        return

    original_decrypt = getattr(client_class, "_decrypt_raw_data", None)
    original_handle = getattr(client_class, "_handle_data_message", None)
    if original_decrypt is None or original_handle is None:
        _LOGGER.warning(
            "firebase-messaging internals changed; compatibility patch skipped"
        )
        return

    def decrypt_with_padding(
        credentials: dict[str, Any],
        crypto_key: str,
        salt: str,
        raw_data: bytes,
    ) -> bytes:
        def padded(value: str) -> str:
            return value + "=" * (-len(value) % 4)

        result = original_decrypt(
            credentials,
            padded(crypto_key),
            padded(salt),
            raw_data,
        )
        return bytes(result)

    def handle_message_safely(instance: Any, *args: Any, **kwargs: Any) -> Any:
        try:
            return original_handle(instance, *args, **kwargs)
        except Exception as error:
            _LOGGER.warning(
                "malformed Firebase message dropped; listener remains active: %s",
                type(error).__name__,
            )
            return None

    client_class._decrypt_raw_data = staticmethod(decrypt_with_padding)
    client_class._handle_data_message = handle_message_safely
    _HARDENED = True
