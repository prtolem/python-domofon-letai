# Experimental SIP call control

Начиная с `v0.3.0`, библиотека умеет подключаться к реальному SIP-вызову, объявленному
push-событием, и управлять его signaling lifecycle.

> Функция experimental. Протокол восстановлен по наблюдаемому поведению и тестируется
> локальными SIP fixtures, но пока не подтверждён sanitized TLS capture-ом реального
> сервера оператора. Не используйте автоматическое открытие двери без отдельного правила
> авторизации и безопасного fallback.

## Что реализовано

- SIP-over-TLS с проверкой сертификата по умолчанию;
- Digest REGISTER с MD5 или SHA-256, `qop=auth` и challenge `401`/`407`;
- ожидание и строгая корреляция настоящего `INVITE` с push-событием;
- `603 Decline`;
- signaling-only `200 OK` с inactive SDP;
- ACK, CANCEL, BYE и deregistration;
- Timer H/L с `T1=0.5` и стандартным интервалом `64*T1=32s`;
- retransmission ответа `200 OK` до ACK или Timer L;
- Record-Route routing для исходящего BYE;
- корректное закрытие активных SIP-вызовов вместе с клиентом.

## Установка

FCM listener использует optional extra `calls`:

```bash
python -m pip install \
  "domofon-letai-api[calls] @ git+https://github.com/prtolem/python-domofon-letai.git@v0.3.0"
```

Сам SIP signaling не добавляет стороннего SIP stack и использует стандартные
`asyncio`/`ssl` primitives.

## Полный lifecycle

```python
import asyncio
import os
from pathlib import Path

from domofon_letai import DomofonLetaiClient, FileFcmCredentialStore


async def main() -> None:
    store = FileFcmCredentialStore(
        Path.home() / ".local/state/domofon-letai/fcm.json"
    )

    async with DomofonLetaiClient(
        os.environ["DOMOFON_LETAI_PHONE"],
        access_token=os.environ["DOMOFON_LETAI_TOKEN"],
    ) as client:
        intercoms = await client.list_intercoms()
        by_login = {
            intercom.sip_login: intercom
            for intercom in intercoms
            if intercom.sip_login
        }

        async with client.incoming_calls(credential_store=store) as pushes:
            async for event in pushes:
                intercom = by_login.get(event.sip_login)
                if intercom is None:
                    continue

                call = await client.connect_incoming_call(event)
                async with call:
                    # Выполните распознавание или другое правило до этой строки.
                    await call.open_door_and_end(intercom.id)


asyncio.run(main())
```

Push нужен только как trigger и источник endpoint/correlation metadata. Метод
`connect_incoming_call()` отдельно получает SIP credentials через authenticated HTTP API,
подключается к TLS endpoint, регистрируется и ждёт настоящий `INVITE`.

## Операции

### Отклонить звонок

```python
await call.decline()
```

Отправляется `603 Decline`. Метод завершается после matching ACK или Timer H. Повторный
final response для того же INVITE не отправляется.

### Принять без разговора

```python
await call.answer_inactive()
```

Отправляется `200 OK` с SDP, в котором каждая предложенная media section получает port
`0` и `a=inactive`. Это устанавливает SIP dialog, но намеренно не создаёт RTP sockets и не
передаёт аудио или видео.

Если ACK не приходит до Timer L, библиотека прекращает retransmission, best-effort
отправляет BYE и выбрасывает `SipTimeoutError`.

### Завершить принятый вызов

```python
await call.answer_inactive()
# Прикладная логика без аудио.
await call.hangup()
```

`hangup()` допустим после успешного ACK. Он отправляет in-dialog `BYE` и ждёт exact final
response этой транзакции. Provisional `1xx` и ответы с другим Via/branch/CSeq игнорируются.

### Открыть дверь и завершить звонок

```python
await call.open_door_and_end(intercom_id, screen_id=1)
```

Команда открытия отправляется ровно один раз и не ретраится. Затем вызов:

- отклоняется, если ещё звонит;
- завершается BYE, если уже принят;
- дожидается текущей termination transaction, если завершение уже началось.

Если HTTP-команда и SIP-завершение не оба успешны, выбрасывается
`OpenDoorAndEndError`. Поля `door_request_succeeded`, `sip_end_succeeded`, `door_error` и
`sip_error` позволяют безопасно решить, что делать дальше. Отмена coroutine не
проглатывается: после shielded best-effort SIP cleanup исходный `CancelledError`
выбрасывается снова.

## Настройки подключения

```python
call = await client.connect_incoming_call(
    event,
    ssl_context=None,
    connect_timeout=10.0,
    invite_timeout=15.0,
    transaction_timeout=32.0,
    t1=0.5,
    strict_endpoint=True,
    strict_correlation=True,
)
```

- `ssl_context` — собственный `ssl.SSLContext`; без него используется системное CA
  хранилище, TLS verification включена, минимум TLS 1.2;
- `connect_timeout` — предел открытия TLS и общего registration stage;
- `invite_timeout` — ожидание matching INVITE после регистрации;
- `transaction_timeout` — bounded timeout для BYE и deregistration;
- `t1` — SIP T1; Timer H/L равны `64*T1`. Менять обычно не требуется;
- `strict_endpoint=True` — push host должен совпасть с authenticated `SipSettings.address`,
  transport должен быть `tls`, порт — `9741`;
- `strict_correlation=True` — Call-ID и SIP login панели должны совпасть с push metadata.

### Controlled overrides

Если реальный payload отличается от текущих предположений, для диагностики можно явно
ослабить одну проверку:

```python
call = await client.connect_incoming_call(
    event,
    strict_endpoint=False,
    strict_correlation=False,
)
```

Это снижает защиту от подключения к подменённому endpoint или обработки чужого INVITE.
Используйте overrides только после проверки payload и не делайте их production default.
UDP и порт `9740` как fallback не используются.

Для частной CA передайте отдельный context вместо отключения проверки:

```python
import ssl

context = ssl.create_default_context(cafile="operator-ca.pem")
call = await client.connect_incoming_call(event, ssl_context=context)
```

## Состояния

`SipIncomingCall.state` принимает значения:

- `RINGING`;
- `ANSWER_SENT`;
- `ESTABLISHED`;
- `DECLINE_SENT`;
- `TERMINATING`;
- `ENDED`;
- `FAILED`;
- `CLOSED`.

`acknowledged` показывает получение ACK для final INVITE response, `last_error` — последнюю
асинхронную signaling error. `wait_ended()` ждёт локальное или удалённое завершение.
Объект рекомендуется всегда использовать через `async with` или закрывать `aclose()`.

## Ошибки

Все SIP-ошибки наследуются от `SipError`:

- `SipMetadataError` — небезопасный или неполный endpoint в push;
- `SipTransportError` — DNS/TLS/stream failure;
- `SipAuthenticationError` — REGISTER credentials/challenge отклонены;
- `SipCallMismatchError` — INVITE не соответствует push или аккаунту;
- `SipProtocolError` — malformed/unsupported SIP message;
- `SipCallStateError` — операция не разрешена в текущем состоянии;
- `SipTimeoutError` — registration, INVITE, ACK, BYE или deregistration не завершились;
- `OpenDoorAndEndError` — HTTP opening и SIP termination не оба успешны.

Credentials, Authorization digest, access token и FCM identity не должны попадать в
логи. Библиотека не включает их в собственные error messages.

## Что проверить на реальном аккаунте

До подтверждения совместимости полезно проверить:

1. совпадает ли push `sip_call_id` с `INVITE Call-ID`;
2. совпадает ли push host с `SipSettings.address`;
3. принимает ли сервер port-zero inactive SDP;
4. завершает ли `603` все forked устройства;
5. корректна ли certificate chain/SNI на endpoint;
6. снимается ли звонок после `open_door_and_end()`.

При несовместимости сохраните sanitized trace без SIP password, Authorization,
access token и FCM credentials. Достаточно start lines, Via/From/To/Call-ID/CSeq,
Record-Route/Contact, статусов и SDP с замаскированными адресами.
