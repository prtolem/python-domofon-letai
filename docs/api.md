# API reference

## `DomofonLetaiClient`

```python
DomofonLetaiClient(
    phone,
    *,
    access_token=None,
    timeout=10.0,
    media_verify=True,
    api_client=None,
    media_client=None,
    device_code="Android_empty_push_token",
)
```

Основные методы:

| Метод | Назначение |
|---|---|
| `request_sms_code()` | запросить код авторизации |
| `confirm_sms_code(code)` | подтвердить код и вернуть access token |
| `list_intercoms()` | получить доступные панели |
| `get_intercom(id)` | найти одну панель и обновить её URL |
| `open_door(id)` | отправить команду открытия |
| `get_stream_source(id, format)` | получить свежий HLS/MPEG-TS URL |
| `open_stream(id, format)` | открыть неблокирующий media stream |
| `get_sip_settings()` | получить SIP account metadata |
| `incoming_calls(...)` | создать listener входящих звонков |
| `aclose()` | закрыть listener-ы и принадлежащие клиенту HTTP connections |

Клиент рекомендуется использовать как async context manager.

## Модели

### `Intercom`

- `id: int`
- `name: str`
- `sip_login: str | None`
- `muted: bool`
- `buildings: tuple[Building, ...]`
- `hls: StreamSource | None`
- `mpeg_ts: StreamSource | None`
- `addresses: tuple[str, ...]`
- `address: str`

### `SipSettings`

- `address: str`
- `port: int`
- `login: str`
- `password: str` — скрыт из `repr`
- `registration_expires: int | None`

### `IncomingCallEvent`

Подробное описание: [incoming-calls.md](incoming-calls.md).

## Потоки

```python
async with client.open_stream(intercom_id, StreamFormat.MPEG_TS) as stream:
    async for chunk in stream.aiter_bytes():
        ...
```

`MediaStream` предоставляет `url`, `status_code`, `content_type`, `headers` и
`aiter_bytes()`. Объект действителен только внутри контекстного менеджера.

## Входящие звонки

```python
listener = client.incoming_calls(
    credential_store=FileFcmCredentialStore(path),
    max_pending_events=32,
)

async with listener:
    event = await anext(listener)
```

Состояния: `NEW`, `STARTING`, `RUNNING`, `FAILED`, `CLOSING`, `CLOSED`.

## Исключения

Все ошибки наследуются от `DomofonLetaiError`:

- `ValidationError`
- `TransportError`
- `ProtocolError`
- `ApiError`
  - `AuthenticationError`
  - `PermissionDeniedError`
  - `NotFoundError`
  - `RateLimitError`
- `StreamError`
  - `StreamNotAvailableError`
  - `StreamAuthenticationError`
- `PushError`
  - `PushDependencyError`
  - `CredentialStoreError`
