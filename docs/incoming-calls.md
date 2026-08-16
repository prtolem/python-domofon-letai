# Входящие звонки

SDK может получать уведомления о входящем звонке через push-канал оператора. Это
подходит для прикладной логики:

- запустить распознавание лица после нажатия кнопки домофона;
- сохранить свежий кадр;
- отправить уведомление в бот;
- открыть дверь после собственной проверки;
- связать звонок с камерой по `sip_login`.

## Установка

Поддержка звонков является опциональной:

```bash
python -m pip install \
  "domofon-letai-api[calls] @ git+https://github.com/prtolem/python-domofon-letai.git@v0.3.0"
```

Extra `calls` устанавливает `firebase-messaging`. Основной HTTP/video API можно
использовать без этой зависимости.

## Полный пример

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
        by_sip_login = {
            intercom.sip_login: intercom
            for intercom in intercoms
            if intercom.sip_login
        }

        async with client.incoming_calls(credential_store=store) as calls:
            async for event in calls:
                intercom = by_sip_login.get(event.sip_login)
                print(
                    "Входящий звонок:",
                    intercom.name if intercom else event.sip_login,
                    event.call_id,
                )

                # Здесь можно взять свежий MPEG-TS кадр, запустить модель,
                # отправить уведомление или вызвать await client.open_door(...).


asyncio.run(main())
```

`IncomingCallListener` является single-consumer async iterator. Один экземпляр listener-а
нужно читать ровно одним `async for`.

## Зачем нужно хранилище credentials

Firebase создаёт идентичность виртуального устройства и возвращает её при первичной
регистрации. Credentials необходимо сохранять между перезапусками; иначе создаются новые
FCM-регистрации, а уведомления могут продолжить уходить на старый token.

`FileFcmCredentialStore`:

- сохраняет данные атомарной заменой файла;
- создаёт новый каталог с правами `0700`;
- создаёт credential-файл с правами `0600` на POSIX;
- не выводит credentials в `repr` или сообщения ошибок;
- отклоняет повреждённый или неизвестный формат вместо молчаливой перезаписи.

Файл не зашифрован. Для Redis, базы данных, Vault или собственного шифрования реализуйте
протокол `FcmCredentialStore`:

```python
from collections.abc import Mapping
from typing import Any


class MyCredentialStore:
    async def load(self) -> dict[str, Any] | None:
        ...

    async def save(self, credentials: Mapping[str, Any]) -> None:
        ...
```

Используйте отдельное хранилище для каждого аккаунта.

## Поля события

`IncomingCallEvent` содержит:

| Поле | Значение |
|---|---|
| `received_at` | время получения в UTC |
| `message_id` | идентификатор push-доставки |
| `call_id` | SIP call ID, если присутствует |
| `notification_uuid` | UUID уведомления оператора |
| `sip_login` | идентификатор панели, например `G17126` |
| `sip_address` | SIP-адрес из push payload |
| `sip_port` | SIP-порт из push payload |
| `sip_transport` | транспорт, обычно `tls` |
| `raw_data` | read-only копия исходных полей |

Необязательные поля могут быть `None`: формат неофициального payload может меняться.

## Доставка и дедупликация

Push-доставка имеет семантику at-least-once. SDK отбрасывает повтор одного
`persistent_id` в памяти текущего процесса, но пользовательская логика с побочными
эффектами всё равно должна быть идемпотентной. После перезапуска in-memory дедупликация
сбрасывается.

Очередь listener-а ограничена 32 событиями по умолчанию. Если consumer перестал читать
её и очередь переполнилась, listener переходит в `FAILED`, а iterator выбрасывает
`PushError`. Размер можно изменить через `max_pending_events`.

## Что означает событие

Событие подтверждает, что оператор объявил начало входящего звонка. Push-канал не
сообщает подтверждённое окончание разговора и не содержит полного SIP INVITE.

Сам `IncomingCallEvent` поэтому нельзя принять или отклонить. Перед управлением вызовом
нужно подключиться к SIP/TLS endpoint, зарегистрироваться и получить соответствующий
`INVITE`:

```python
call = await client.connect_incoming_call(event)
async with call:
    await call.decline()
```

Также доступны `answer_inactive()`, `hangup()` и `open_door_and_end()`. Реализация пока
experimental и не предоставляет RTP/audio. Полное описание, модель безопасности и
варианты настройки: [sip-call-control.md](sip-call-control.md).

## Сеть и приватность

При включении listener-а выполняются соединения с Firebase/Google для регистрации и
доставки сообщений, а FCM token передаётся API Таттелекома. Push payload и device
credentials обрабатываются сторонней инфраструктурой. Учитывайте это при выборе места
запуска и политики хранения данных.

## Ошибки

- `PushDependencyError` — extra `calls` не установлен;
- `CredentialStoreError` — credentials невозможно прочитать или сохранить;
- `PushError` — сбой Firebase listener-а или переполнение очереди;
- стандартные `AuthenticationError`, `RateLimitError`, `TransportError` — ошибка при
  регистрации FCM token у оператора;
- ошибки подключения и SIP-диалога описаны в
  [sip-call-control.md](sip-call-control.md#ошибки).

`listener.last_error` содержит последнюю фоновую ошибку, а `listener.state` — текущее
состояние lifecycle.
