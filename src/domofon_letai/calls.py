"""Experimental push-triggered SIP call control."""

from __future__ import annotations

import asyncio
import ssl
from collections.abc import Awaitable, Callable
from contextlib import suppress
from enum import Enum
from typing import TYPE_CHECKING, Protocol

from .exceptions import (
    OpenDoorAndEndError,
    SipAuthenticationError,
    SipCallMismatchError,
    SipCallStateError,
    SipError,
    SipMetadataError,
    SipProtocolError,
    SipTimeoutError,
)
from .models import IncomingCallEvent, SipSettings
from .sip import (
    ByeBuilder,
    RegisterBuilder,
    SipMessage,
    SipResponseBuilder,
    SipTlsConnection,
    TransactionKey,
    build_inactive_sdp_answer,
    build_ok_response,
    build_request_terminated_response,
    build_response,
    parse_header_tag,
    parse_sip_uri,
    transaction_key,
)

if TYPE_CHECKING:
    from .client import DomofonLetaiClient


class SipCallState(str, Enum):
    """State of an experimental SIP signaling session."""

    RINGING = "ringing"
    ANSWER_SENT = "answer_sent"
    ESTABLISHED = "established"
    DECLINE_SENT = "decline_sent"
    TERMINATING = "terminating"
    ENDED = "ended"
    FAILED = "failed"
    CLOSED = "closed"


class SipConnection(Protocol):
    """Transport boundary used by the call controller and its tests."""

    @property
    def local_host(self) -> str: ...

    @property
    def local_port(self) -> int: ...

    async def send(self, message: SipMessage | bytes) -> None: ...

    async def receive(self) -> SipMessage: ...

    async def close(self) -> None: ...


ConnectionFactory = Callable[
    [str, int, ssl.SSLContext, float | None],
    Awaitable[SipConnection],
]


class SipIncomingCall:
    """An actual SIP INVITE correlated with an incoming-call push event.

    Media is intentionally unsupported. ``answer_inactive`` establishes a SIP dialog
    while rejecting every offered RTP stream with port zero.
    """

    def __init__(
        self,
        client: DomofonLetaiClient,
        event: IncomingCallEvent,
        settings: SipSettings,
        connection: SipConnection,
        invite: SipMessage,
        response_builder: SipResponseBuilder,
        bye_builder: ByeBuilder,
        register_builder: RegisterBuilder,
        register_challenge: SipMessage | None,
        *,
        transaction_timeout: float,
        t1: float,
    ) -> None:
        self._client = client
        self._event = event
        self._settings = settings
        self._connection = connection
        self._invite = invite
        self._invite_key = transaction_key(invite)
        self._response_builder = response_builder
        self._bye_builder = bye_builder
        self._register_builder = register_builder
        self._register_challenge = register_challenge
        self._transaction_timeout = transaction_timeout
        self._t1 = t1
        self._timer_h = 64 * t1
        self._timer_l = 64 * t1
        self._state = SipCallState.RINGING
        self._last_error: BaseException | None = None
        self._final_invite_response: SipMessage | None = None
        self._ringing_response = response_builder.ringing()
        self._invite_transaction_active = True
        self._dialog_established = False
        self._cancel_responses: dict[TransactionKey, SipMessage] = {}
        self._remote_bye_responses: dict[TransactionKey, SipMessage] = {}
        self._invite_lock = asyncio.Lock()
        self._bye_lock = asyncio.Lock()
        self._close_lock = asyncio.Lock()
        self._closing = False
        self._ack_event = asyncio.Event()
        self._ended_event = asyncio.Event()
        self._failure_event = asyncio.Event()
        self._closed_event = asyncio.Event()
        self._bye_response: asyncio.Future[int] = (
            asyncio.get_running_loop().create_future()
        )
        self._local_bye_key: TransactionKey | None = None
        self._deregister_key: TransactionKey | None = None
        self._deregister_response: asyncio.Future[SipMessage] | None = None
        self._answer_deadline: float | None = None
        self._answer_retransmit_task: asyncio.Task[None] | None = None
        self._timer_h_task: asyncio.Task[None] | None = None
        self._timer_l_task: asyncio.Task[None] | None = None
        self._remote_tag = _required_tag(invite, "From")
        self._local_tag = response_builder.local_tag
        self._remote_cseq = _required_cseq(invite)
        self._receive_task = asyncio.create_task(self._receive_loop())
        try:
            self._client._register_sip_call(self)
        except Exception:
            self._receive_task.cancel()
            raise

    @property
    def event(self) -> IncomingCallEvent:
        """Push event that triggered this SIP session."""

        return self._event

    @property
    def call_id(self) -> str:
        """SIP dialog Call-ID."""

        return self._invite_key[0]

    @property
    def state(self) -> SipCallState:
        """Current signaling state."""

        return self._state

    @property
    def acknowledged(self) -> bool:
        """Whether an ACK for the INVITE final response was received."""

        return self._ack_event.is_set()

    @property
    def last_error(self) -> BaseException | None:
        """Last asynchronous signaling failure."""

        return self._last_error

    async def __aenter__(self) -> SipIncomingCall:
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.aclose()

    async def decline(self) -> None:
        """Send ``603 Decline`` and keep the transaction alive for its ACK."""

        await self._send_non_2xx_final(self._response_builder.decline())
        await self._wait_for_ended("timed out waiting for ACK to 603 Decline")

    async def answer_inactive(self) -> None:
        """Answer the call while rejecting all offered RTP media streams."""

        self._ensure_operable()
        async with self._invite_lock:
            self._require_state(SipCallState.RINGING)
            body = build_inactive_sdp_answer(
                self._invite.body,
                origin_address=self._connection.local_host,
            )
            local = _format_contact_host(self._connection.local_host)
            contact = (
                f"<sip:{self._settings.login}@{local}:"
                f"{self._connection.local_port};transport=tls>"
            )
            response = self._response_builder.ok(
                body=body,
                extra_headers=(
                    ("Contact", contact),
                    ("Content-Type", "application/sdp"),
                    (
                        "Allow",
                        "INVITE, ACK, CANCEL, OPTIONS, BYE, INFO, UPDATE",
                    ),
                ),
            )
            self._final_invite_response = response
            self._dialog_established = True
            self._state = SipCallState.ANSWER_SENT
            self._answer_deadline = asyncio.get_running_loop().time() + self._timer_l
            await self._connection.send(response)
            self._answer_retransmit_task = asyncio.create_task(
                self._retransmit_answer(response)
            )
            self._timer_l_task = asyncio.create_task(self._complete_timer_l())

        try:
            await self._wait_for_signal(
                self._ack_event,
                "timed out waiting for SIP ACK",
                timeout=self._remaining_answer_time(),
            )
        except SipTimeoutError:
            if self._timer_l_task is not None:
                await asyncio.shield(self._timer_l_task)
            error = self._last_error
            if isinstance(error, SipTimeoutError):
                raise error from None
            raise

        if self._state is SipCallState.ANSWER_SENT:
            self._state = SipCallState.ESTABLISHED

    async def hangup(self) -> None:
        """Send an in-dialog BYE and await its exact final response."""

        await self._hangup(allow_closing=False)

    async def _hangup(self, *, allow_closing: bool) -> None:
        self._ensure_operable(allow_closing=allow_closing)
        async with self._bye_lock:
            if self._state is SipCallState.TERMINATING:
                pass
            else:
                self._require_state(SipCallState.ESTABLISHED)
                bye = self._bye_builder.build()
                self._local_bye_key = transaction_key(bye)
                self._state = SipCallState.TERMINATING
                await self._connection.send(bye)
                self._dialog_established = False
                self._ended_event.set()

        status = await self._wait_for_bye_response()
        if self._state is not SipCallState.CLOSED:
            self._state = SipCallState.ENDED

        if not 200 <= status < 300:
            error = SipProtocolError(f"SIP server rejected BYE with status {status}")
            self._last_error = error
            raise error

    async def open_door_and_end(
        self,
        intercom_id: int,
        *,
        screen_id: int = 1,
    ) -> None:
        """Request door opening and terminate the SIP call without retries."""

        door_error: Exception | None = None
        sip_error: Exception | None = None
        door_succeeded = False
        sip_succeeded = False

        try:
            await self._client.open_door(intercom_id, screen_id=screen_id)
            door_succeeded = True
        except asyncio.CancelledError:
            await self._shielded_end_after_cancellation()
            raise
        except Exception as error:
            door_error = error

        try:
            await self._end_for_current_state()
            sip_succeeded = True
        except asyncio.CancelledError:
            await self._shielded_end_after_cancellation()
            raise
        except Exception as error:
            sip_error = error

        if door_error is not None or sip_error is not None:
            raise OpenDoorAndEndError(
                "door request and SIP termination did not both succeed",
                door_request_succeeded=door_succeeded,
                sip_end_succeeded=sip_succeeded,
                door_error=door_error,
                sip_error=sip_error,
            )

    async def wait_ended(self) -> None:
        """Wait until the remote or local side ends the call."""

        await self._ended_event.wait()

    async def aclose(self) -> None:
        """Gracefully terminate signaling, deregister, and close TLS."""

        async with self._close_lock:
            if self._state is SipCallState.CLOSED:
                return
            self._closing = True

            with suppress(SipError):
                if self._state is SipCallState.RINGING:
                    await self._send_non_2xx_final(
                        self._response_builder.decline(),
                        allow_closing=True,
                    )
                    await self._wait_event_bounded(self._ended_event, self._timer_h)
                elif self._state is SipCallState.DECLINE_SENT:
                    await self._wait_event_bounded(self._ended_event, self._timer_h)
                elif self._state is SipCallState.ANSWER_SENT:
                    await self._wait_event_bounded(
                        self._ack_event,
                        self._remaining_answer_time(),
                    )
                    if self._ack_event.is_set():
                        self._state = SipCallState.ESTABLISHED
                        await self._hangup(allow_closing=True)
                    elif self._timer_l_task is not None:
                        await self._timer_l_task
                elif self._state is SipCallState.ESTABLISHED:
                    await self._hangup(allow_closing=True)
                elif self._state is SipCallState.TERMINATING:
                    await self._wait_for_bye_response()

            await self._best_effort_deregister()
            await self._cancel_protocol_tasks()
            with suppress(SipError):
                await self._connection.close()

            self._state = SipCallState.CLOSED
            self._ended_event.set()
            self._closed_event.set()
            self._client._discard_sip_call(self)

    async def _send_non_2xx_final(
        self,
        response: SipMessage,
        *,
        allow_closing: bool = False,
    ) -> None:
        self._ensure_operable(allow_closing=allow_closing)
        async with self._invite_lock:
            self._require_state(SipCallState.RINGING)
            self._final_invite_response = response
            self._state = SipCallState.DECLINE_SENT
            await self._connection.send(response)
            self._start_non_2xx_timer()

    def _start_non_2xx_timer(self) -> None:
        if self._timer_h_task is None:
            self._timer_h_task = asyncio.create_task(self._complete_timer_h())

    async def _complete_timer_h(self) -> None:
        await self._wait_event_bounded(self._ack_event, self._timer_h)
        self._invite_transaction_active = False
        if self._state is SipCallState.DECLINE_SENT:
            self._state = SipCallState.ENDED
        self._ended_event.set()

    async def _complete_timer_l(self) -> None:
        await asyncio.sleep(self._remaining_answer_time())
        self._invite_transaction_active = False
        if self._ack_event.is_set() or self._state is SipCallState.CLOSED:
            return

        error = SipTimeoutError("timed out waiting for SIP ACK")
        self._last_error = error
        try:
            await self._best_effort_bye_after_ack_timeout()
        finally:
            self._state = SipCallState.FAILED
            self._failure_event.set()
            self._ended_event.set()

    async def _retransmit_answer(self, response: SipMessage) -> None:
        interval = self._t1
        while not self._ack_event.is_set():
            remaining = self._remaining_answer_time()
            if remaining <= 0:
                return
            try:
                await asyncio.wait_for(
                    self._ack_event.wait(),
                    timeout=min(interval, remaining),
                )
            except asyncio.TimeoutError:
                if not self._ack_event.is_set() and self._remaining_answer_time() > 0:
                    await self._connection.send(response)
                interval = min(interval * 2, 4.0)

    async def _receive_loop(self) -> None:
        try:
            while self._state not in (SipCallState.CLOSED, SipCallState.FAILED):
                message = await self._connection.receive()
                await self._handle_message(message)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            if not self._closing and self._state is not SipCallState.ENDED:
                self._last_error = error
                self._state = SipCallState.FAILED
                self._failure_event.set()
                self._ended_event.set()
                if not self._bye_response.done():
                    self._bye_response.set_exception(error)
                deregister = self._deregister_response
                if deregister is not None and not deregister.done():
                    deregister.set_exception(error)

    async def _handle_message(self, message: SipMessage) -> None:
        if message.status_code is not None:
            status = message.status_code
            if status >= 200:
                if self._matches_deregister_response(message):
                    deregister_future = self._deregister_response
                    if deregister_future is not None and not deregister_future.done():
                        deregister_future.set_result(message)
                elif (
                    self._matches_bye_response(message)
                    and not self._bye_response.done()
                ):
                    self._bye_response.set_result(status)
            return

        method = (message.method or "").upper()
        if method == "ACK":
            if self._matches_ack(message):
                self._ack_event.set()
                if self._answer_retransmit_task is not None:
                    self._answer_retransmit_task.cancel()
                if self._state is SipCallState.ANSWER_SENT:
                    self._state = SipCallState.ESTABLISHED
                elif self._state is SipCallState.DECLINE_SENT:
                    self._invite_transaction_active = False
                    self._state = SipCallState.ENDED
                    self._ended_event.set()
            return

        if method == "INVITE":
            if (
                self._invite_transaction_active
                and transaction_key(message) == self._invite_key
            ):
                invite_response = self._final_invite_response or self._ringing_response
                await self._connection.send(invite_response)
            elif self._matches_dialog(message):
                await self._connection.send(
                    build_response(
                        message,
                        491,
                        "Request Pending",
                        local_tag=self._local_tag,
                    )
                )
            return

        if method == "CANCEL":
            await self._handle_cancel(message)
            return

        if method == "BYE":
            await self._handle_bye(message)
            return

        if method == "OPTIONS":
            await self._connection.send(
                build_ok_response(
                    message,
                    extra_headers=(("Allow", "INVITE, ACK, CANCEL, OPTIONS, BYE"),),
                )
            )

    async def _handle_cancel(self, cancel: SipMessage) -> None:
        try:
            key = transaction_key(cancel)
        except SipProtocolError:
            return

        async with self._invite_lock:
            response = self._cancel_responses.get(key)
            terminated: SipMessage | None = None
            if response is None:
                matches = self._invite_transaction_active and _same_invite_transaction(
                    cancel,
                    self._invite,
                )
                if matches:
                    response = build_ok_response(cancel, local_tag=self._local_tag)
                    if self._final_invite_response is None:
                        terminated = build_request_terminated_response(
                            self._invite,
                            local_tag=self._local_tag,
                        )
                        self._final_invite_response = terminated
                        self._state = SipCallState.DECLINE_SENT
                else:
                    response = build_response(
                        cancel,
                        481,
                        "Call/Transaction Does Not Exist",
                    )
                self._cancel_responses[key] = response

            await self._connection.send(response)
            if terminated is not None:
                await self._connection.send(terminated)
                self._start_non_2xx_timer()

    async def _handle_bye(self, bye: SipMessage) -> None:
        try:
            key = transaction_key(bye)
        except SipProtocolError:
            await self._connection.send(build_response(bye, 400, "Bad Request"))
            return

        cached = self._remote_bye_responses.get(key)
        if cached is not None:
            await self._connection.send(cached)
            return

        if not self._dialog_established or not self._matches_dialog(bye):
            response = build_response(
                bye,
                481,
                "Call/Transaction Does Not Exist",
            )
            self._remote_bye_responses[key] = response
            await self._connection.send(response)
            return

        cseq = _required_cseq(bye)
        if cseq <= self._remote_cseq:
            response = build_response(
                bye,
                500,
                "Server Internal Error",
                local_tag=self._local_tag,
            )
            self._remote_bye_responses[key] = response
            await self._connection.send(response)
            return

        self._remote_cseq = cseq
        response = build_ok_response(bye, local_tag=self._local_tag)
        self._remote_bye_responses[key] = response
        await self._connection.send(response)
        self._dialog_established = False
        self._state = SipCallState.ENDED
        self._ended_event.set()

    def _matches_ack(self, message: SipMessage) -> bool:
        try:
            key = transaction_key(message)
        except SipProtocolError:
            return False
        if (
            key[0] != self.call_id
            or key[1] != self._invite_key[1]
            or key[2] != "ACK"
        ):
            return False
        if self._state is SipCallState.DECLINE_SENT:
            return (
                key[3] == self._invite_key[3]
                and key[4] == self._invite_key[4]
            )
        return self._dialog_established and self._matches_dialog(message)

    def _matches_dialog(self, message: SipMessage) -> bool:
        if message.call_id != self.call_id:
            return False
        from_value = message.get_header("From")
        to_value = message.get_header("To")
        if from_value is None or to_value is None:
            return False
        return (
            parse_header_tag(from_value) == self._remote_tag
            and parse_header_tag(to_value) == self._local_tag
        )

    def _matches_bye_response(self, message: SipMessage) -> bool:
        if self._local_bye_key is None:
            return False
        try:
            return transaction_key(message) == self._local_bye_key
        except SipProtocolError:
            return False

    def _matches_deregister_response(self, message: SipMessage) -> bool:
        if self._deregister_key is None:
            return False
        try:
            return transaction_key(message) == self._deregister_key
        except SipProtocolError:
            return False

    async def _wait_for_signal(
        self,
        event: asyncio.Event,
        timeout_message: str,
        *,
        timeout: float | None = None,
    ) -> None:
        event_task = asyncio.create_task(event.wait())
        failure_task = asyncio.create_task(self._failure_event.wait())
        closed_task = asyncio.create_task(self._closed_event.wait())
        tasks = (event_task, failure_task, closed_task)
        try:
            done, _ = await asyncio.wait(
                tasks,
                timeout=self._transaction_timeout if timeout is None else timeout,
                return_when=asyncio.FIRST_COMPLETED,
            )
            if not done:
                raise SipTimeoutError(timeout_message)
            if failure_task in done:
                error = self._last_error
                if isinstance(error, SipError):
                    raise error
                raise SipProtocolError("SIP receive loop failed") from error
            if closed_task in done and not event.is_set():
                raise SipCallStateError("SIP call was closed during the operation")
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()

    async def _wait_for_ended(self, timeout_message: str) -> None:
        await self._wait_for_signal(self._ended_event, timeout_message)

    async def _wait_for_bye_response(self) -> int:
        response_task = asyncio.ensure_future(asyncio.shield(self._bye_response))
        closed_task = asyncio.create_task(self._closed_event.wait())
        try:
            done, _ = await asyncio.wait(
                (response_task, closed_task),
                timeout=self._transaction_timeout,
                return_when=asyncio.FIRST_COMPLETED,
            )
            if not done:
                raise SipTimeoutError("timed out waiting for BYE response")
            if closed_task in done and not self._bye_response.done():
                raise SipCallStateError("SIP call closed before BYE response")
            return await response_task
        finally:
            if not response_task.done():
                response_task.cancel()
            if not closed_task.done():
                closed_task.cancel()

    async def _wait_event_bounded(
        self,
        event: asyncio.Event,
        timeout: float,
    ) -> None:
        with suppress(asyncio.TimeoutError):
            await asyncio.wait_for(event.wait(), timeout=timeout)

    async def _best_effort_deregister(self) -> None:
        with suppress(SipError):
            deadline = asyncio.get_running_loop().time() + self._transaction_timeout
            challenge = self._register_challenge
            for attempt in range(2):
                request = self._register_builder.build_deregister(challenge)
                response = await self._send_and_wait_deregister(request, deadline)
                if response.status_code == 200:
                    return
                if response.status_code in (401, 407) and attempt == 0:
                    challenge = response
                    continue
                raise SipProtocolError(
                    f"SIP deregistration returned status {response.status_code}"
                )

    async def _send_and_wait_deregister(
        self,
        request: SipMessage,
        deadline: float,
    ) -> SipMessage:
        remaining = deadline - asyncio.get_running_loop().time()
        if remaining <= 0:
            raise SipTimeoutError("timed out waiting for SIP deregistration")

        future: asyncio.Future[SipMessage] = (
            asyncio.get_running_loop().create_future()
        )
        self._deregister_key = transaction_key(request)
        self._deregister_response = future
        try:
            await self._connection.send(request)
            try:
                return await asyncio.wait_for(asyncio.shield(future), timeout=remaining)
            except asyncio.TimeoutError as error:
                raise SipTimeoutError(
                    "timed out waiting for SIP deregistration"
                ) from error
        finally:
            self._deregister_key = None
            self._deregister_response = None
            if not future.done():
                future.cancel()

    async def _best_effort_bye_after_ack_timeout(self) -> None:
        with suppress(Exception):
            if not self._dialog_established or self._state is SipCallState.CLOSED:
                return
            async with self._bye_lock:
                if self._local_bye_key is None:
                    bye = self._bye_builder.build()
                    self._local_bye_key = transaction_key(bye)
                    self._state = SipCallState.TERMINATING
                    await self._connection.send(bye)
                    self._dialog_established = False
                    self._ended_event.set()
            await self._wait_for_bye_response()

    def _remaining_answer_time(self) -> float:
        if self._answer_deadline is None:
            return self._timer_l
        return max(0.0, self._answer_deadline - asyncio.get_running_loop().time())

    async def _cancel_protocol_tasks(self) -> None:
        tasks = (
            self._answer_retransmit_task,
            self._timer_h_task,
            self._timer_l_task,
            self._receive_task,
        )
        for task in tasks:
            if task is not None and not task.done():
                task.cancel()
        for task in tasks:
            if task is not None:
                with suppress(asyncio.CancelledError, Exception):
                    await task

    async def _end_for_current_state(self) -> None:
        if self._state is SipCallState.RINGING:
            await self.decline()
        elif self._state is SipCallState.ESTABLISHED:
            await self.hangup()
        elif self._state is SipCallState.ANSWER_SENT:
            await self._wait_for_signal(
                self._ack_event,
                "timed out waiting for SIP ACK",
            )
            if self._state is SipCallState.ANSWER_SENT:
                self._state = SipCallState.ESTABLISHED
            await self.hangup()
        elif self._state is SipCallState.TERMINATING:
            await self._wait_for_bye_response()
        elif self._state is SipCallState.DECLINE_SENT:
            await self._wait_for_ended("timed out waiting for decline ACK")
        elif self._state is not SipCallState.ENDED:
            raise SipCallStateError(
                f"cannot end a call in state {self._state.value}"
            )

    async def _shielded_end_after_cancellation(self) -> None:
        task = asyncio.create_task(self._end_for_current_state())
        with suppress(Exception):
            await asyncio.shield(task)

    def _ensure_operable(self, *, allow_closing: bool = False) -> None:
        if self._state is SipCallState.CLOSED or (
            self._closing and not allow_closing
        ):
            raise SipCallStateError("SIP call is closing or closed")

    def _require_state(self, expected: SipCallState) -> None:
        if self._state is not expected:
            raise SipCallStateError(
                f"call state is {self._state.value}, expected {expected.value}"
            )


async def connect_sip_call(
    client: DomofonLetaiClient,
    event: IncomingCallEvent,
    settings: SipSettings,
    *,
    ssl_context: ssl.SSLContext | None = None,
    connect_timeout: float = 10.0,
    invite_timeout: float = 15.0,
    transaction_timeout: float = 32.0,
    t1: float = 0.5,
    strict_endpoint: bool = True,
    strict_correlation: bool = True,
    _connection_factory: ConnectionFactory | None = None,
) -> SipIncomingCall:
    """Open a push-triggered TLS flow, register, and receive a matching INVITE."""

    if connect_timeout <= 0 or invite_timeout <= 0 or transaction_timeout <= 0:
        raise ValueError("SIP timeouts must be positive")
    if t1 <= 0:
        raise ValueError("SIP T1 must be positive")

    host, port = _validate_endpoint(event, settings, strict_endpoint=strict_endpoint)
    context = ssl_context or _default_ssl_context()
    factory = _connection_factory or _open_tls_connection

    try:
        connection = await factory(host, port, context, connect_timeout)
    except SipError:
        raise
    except Exception as error:
        raise SipMetadataError(
            f"failed to create SIP connection: {type(error).__name__}"
        ) from error

    try:
        register = RegisterBuilder(
            settings.login,
            settings.password,
            host,
            port,
            connection.local_host,
            connection.local_port,
            domain=settings.address,
            expires=settings.registration_expires or 3600,
        )
        registration = asyncio.create_task(
            _register_and_wait_invite(
                connection,
                register,
                event,
                settings,
                strict_correlation=strict_correlation,
                invite_timeout=invite_timeout,
            )
        )
        try:
            invite, register_challenge, pending_messages = await asyncio.wait_for(
                registration,
                timeout=connect_timeout + invite_timeout,
            )
        except asyncio.TimeoutError as error:
            registration.cancel()
            with suppress(asyncio.CancelledError, asyncio.TimeoutError):
                await registration
            raise SipTimeoutError("SIP registration or INVITE timed out") from error

        responses = SipResponseBuilder(invite)
        bye_builder = ByeBuilder(
            invite,
            connection.local_host,
            connection.local_port,
            local_tag=responses.local_tag,
            initial_cseq=1,
        )
        await connection.send(responses.trying())
        await connection.send(responses.ringing())
        call = SipIncomingCall(
            client,
            event,
            settings,
            connection,
            invite,
            responses,
            bye_builder,
            register,
            register_challenge,
            transaction_timeout=transaction_timeout,
            t1=t1,
        )
        try:
            for message in pending_messages:
                await call._handle_message(message)
        except BaseException:
            await call.aclose()
            raise
        return call
    except BaseException:
        with suppress(SipError):
            await connection.close()
        raise


async def _register_and_wait_invite(
    connection: SipConnection,
    register: RegisterBuilder,
    event: IncomingCallEvent,
    settings: SipSettings,
    *,
    strict_correlation: bool,
    invite_timeout: float,
) -> tuple[SipMessage, SipMessage | None, list[SipMessage]]:
    initial = register.build_initial()
    await connection.send(initial)
    first, pending = await _wait_for_register_response(
        connection,
        initial,
        event,
        settings,
        strict_correlation=strict_correlation,
    )

    challenge: SipMessage | None = None
    if first.status_code in (401, 407):
        challenge = first
        authenticated = register.build_authenticated(first)
        await connection.send(authenticated)
        final, pending = await _wait_for_register_response(
            connection,
            authenticated,
            event,
            settings,
            strict_correlation=strict_correlation,
            pending_messages=pending,
        )
    else:
        final = first

    if final.status_code in (401, 403, 407):
        raise SipAuthenticationError(
            f"SIP registration rejected with status {final.status_code}"
        )
    if final.status_code != 200:
        raise SipProtocolError(
            f"SIP registration returned status {final.status_code}"
        )

    if pending:
        return pending[0], challenge, pending[1:]
    invite = await asyncio.wait_for(
        _wait_for_invite(
            connection,
            event,
            settings,
            strict_correlation=strict_correlation,
        ),
        timeout=invite_timeout,
    )
    return invite, challenge, []


async def _wait_for_register_response(
    connection: SipConnection,
    request: SipMessage,
    event: IncomingCallEvent,
    settings: SipSettings,
    *,
    strict_correlation: bool,
    pending_messages: list[SipMessage] | None = None,
) -> tuple[SipMessage, list[SipMessage]]:
    expected_key = transaction_key(request)
    if pending_messages is None:
        pending_messages = []
    unexpected_invites = 0
    while True:
        message = await connection.receive()
        if message.status_code is not None:
            try:
                if transaction_key(message) != expected_key:
                    continue
            except SipProtocolError:
                continue
            if message.status_code < 200 and message.status_code not in (401, 407):
                continue
            return message, pending_messages

        method = (message.method or "").upper()
        if method == "INVITE":
            if _invite_matches(
                message,
                event,
                settings,
                strict_correlation=strict_correlation,
            ):
                if not pending_messages:
                    pending_messages.append(message)
            else:
                unexpected_invites += 1
                await _reject_unexpected_invite(connection, message)
                if unexpected_invites >= 8:
                    raise SipCallMismatchError("too many unrelated SIP INVITEs")
        elif method == "CANCEL" and pending_messages:
            pending_messages.append(message)
        elif method == "OPTIONS":
            await connection.send(build_ok_response(message))


async def _wait_for_invite(
    connection: SipConnection,
    event: IncomingCallEvent,
    settings: SipSettings,
    *,
    strict_correlation: bool,
) -> SipMessage:
    unexpected_invites = 0
    while True:
        message = await connection.receive()
        method = (message.method or "").upper()
        if method == "INVITE":
            if _invite_matches(
                message,
                event,
                settings,
                strict_correlation=strict_correlation,
            ):
                return message
            unexpected_invites += 1
            await _reject_unexpected_invite(connection, message)
            if unexpected_invites >= 8:
                raise SipCallMismatchError("too many unrelated SIP INVITEs")
        elif method == "OPTIONS":
            await connection.send(build_ok_response(message))


async def _reject_unexpected_invite(
    connection: SipConnection,
    invite: SipMessage,
) -> None:
    with suppress(SipProtocolError):
        await connection.send(
            build_response(invite, 481, "Call/Transaction Does Not Exist")
        )


def _invite_matches(
    invite: SipMessage,
    event: IncomingCallEvent,
    settings: SipSettings,
    *,
    strict_correlation: bool,
) -> bool:
    try:
        _validate_invite(
            invite,
            event,
            settings,
            strict_correlation=strict_correlation,
        )
        return True
    except (SipCallMismatchError, SipProtocolError):
        return False


def _validate_endpoint(
    event: IncomingCallEvent,
    settings: SipSettings,
    *,
    strict_endpoint: bool,
) -> tuple[str, int]:
    host = event.sip_address
    port = event.sip_port
    transport = (event.sip_transport or "").casefold()
    if not host or port is None:
        raise SipMetadataError("incoming-call event has no SIP endpoint")
    if transport != "tls":
        raise SipMetadataError("only push-triggered SIP-over-TLS is supported")
    if not 1 <= port <= 65535:
        raise SipMetadataError("incoming-call event has an invalid SIP port")

    if strict_endpoint:
        push_host = host.rstrip(".").casefold()
        settings_host = settings.address.rstrip(".").casefold()
        if push_host != settings_host:
            raise SipMetadataError(
                "push SIP host does not match authenticated SIP settings"
            )
        if port != 9741:
            raise SipMetadataError("unexpected SIP TLS port")
    return host, port


def _validate_invite(
    invite: SipMessage,
    event: IncomingCallEvent,
    settings: SipSettings,
    *,
    strict_correlation: bool,
) -> None:
    if (invite.method or "").upper() != "INVITE":
        raise SipProtocolError("expected SIP INVITE")
    _ = transaction_key(invite)
    _ = _required_tag(invite, "From")
    _ = parse_sip_uri(_required_header(invite, "Contact"))

    to_value = _required_header(invite, "To")
    from_value = _required_header(invite, "From")
    if _uri_user(parse_sip_uri(to_value)) != settings.login:
        raise SipCallMismatchError("INVITE is addressed to another SIP account")

    if strict_correlation:
        if event.call_id and invite.call_id != event.call_id:
            raise SipCallMismatchError("INVITE Call-ID does not match push call ID")
        if event.sip_login and _uri_user(parse_sip_uri(from_value)) != event.sip_login:
            raise SipCallMismatchError("INVITE caller does not match push panel")


def _same_invite_transaction(
    request: SipMessage,
    invite: SipMessage,
) -> bool:
    try:
        key = transaction_key(request)
        invite_key = transaction_key(invite)
        request_uri = _request_uri(request)
        invite_uri = _request_uri(invite)
        from_value = _required_header(request, "From")
        invite_from = _required_header(invite, "From")
        to_value = _required_header(request, "To")
        invite_to = _required_header(invite, "To")
    except SipProtocolError:
        return False
    return (
        key[0] == invite_key[0]
        and key[1] == invite_key[1]
        and key[2] == "CANCEL"
        and key[3] == invite_key[3]
        and key[4] == invite_key[4]
        and request_uri == invite_uri
        and from_value.strip() == invite_from.strip()
        and to_value.strip() == invite_to.strip()
    )


def _request_uri(message: SipMessage) -> str:
    parts = message.start_line.split()
    if len(parts) != 3 or parts[2].casefold() != "sip/2.0":
        raise SipProtocolError("Malformed SIP request line")
    return parts[1]


def _required_header(message: SipMessage, name: str) -> str:
    value = message.get_header(name)
    if value is None:
        raise SipProtocolError(f"SIP message lacks {name}")
    return value


def _required_tag(message: SipMessage, name: str) -> str:
    tag = parse_header_tag(_required_header(message, name))
    if not tag:
        raise SipProtocolError(f"SIP {name} header lacks a tag")
    return tag


def _required_cseq(message: SipMessage) -> int:
    number = message.cseq_number
    if number is None:
        raise SipProtocolError("SIP message has an invalid CSeq")
    return number


def _uri_user(uri: str) -> str:
    value = uri.split(":", 1)[1]
    user, separator, _host = value.partition("@")
    if not separator or not user:
        raise SipProtocolError("SIP URI has no user part")
    return user


def _default_ssl_context() -> ssl.SSLContext:
    context = ssl.create_default_context()
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    return context


async def _open_tls_connection(
    host: str,
    port: int,
    context: ssl.SSLContext,
    timeout: float | None,
) -> SipConnection:
    return await SipTlsConnection.connect(host, port, context, timeout)


def _format_contact_host(host: str) -> str:
    return f"[{host}]" if ":" in host and not host.startswith("[") else host
