# domofon-letai-api

Неофициальный асинхронный Python-клиент для API «Домофон Летай» Таттелекома.
Подходит для ботов, веб-сервисов, автоматизации, обработки видео и реакции на
входящие звонки домофона.

> Проект не связан с ПАО «Таттелеком». API не документирован производителем и может
> измениться без предупреждения.

## Возможности

- вход по номеру телефона и SMS;
- повторное использование сохранённого `access_token`;
- получение списка домофонов и адресов;
- открытие двери;
- получение событий входящего звонка через async iterator;
- получение SIP account metadata для внешних SIP-клиентов;
- получение свежих HLS и MPEG-TS URL;
- неблокирующее чтение MPEG-TS без накопления потока в памяти;
- типизированные модели и структурированные исключения;
- возможность передать собственные `httpx.AsyncClient`.

## Важное предупреждение об авторизации

Запрос новой SMS-сессии может разлогинить официальное приложение «Домофон Летай» на
том же номере. Для автоматизации лучше создать приглашённого пользователя и хранить его
`access_token` во внешнем секрет-хранилище.

## Установка

После публикации репозитория:

```bash
python -m pip install \
  "domofon-letai-api @ git+https://github.com/prtolem/python-domofon-letai.git@v0.2.0"
```

Для разработки из локальной копии:

```bash
git clone https://github.com/prtolem/python-domofon-letai.git
cd python-domofon-letai
python -m pip install -e ".[test,calls]"
```

Для входящих звонков установите optional extra:

```bash
python -m pip install \
  "domofon-letai-api[calls] @ git+https://github.com/prtolem/python-domofon-letai.git@v0.2.0"
```

Рекомендуется устанавливать конкретный tag или commit, а не плавающую ветку `main`.

## Быстрый старт

### Первый вход

```python
import asyncio

from domofon_letai import DomofonLetaiClient


async def main() -> None:
    async with DomofonLetaiClient("+7 900 000-00-00") as client:
        await client.request_sms_code()
        code = input("Код из SMS: ")
        token = await client.confirm_sms_code(code)

        # Сохраните токен безопасно. Не печатайте его в production-логах.
        print(token)

        for intercom in await client.list_intercoms():
            print(intercom.id, intercom.name, intercom.addresses)


asyncio.run(main())
```

### Повторный запуск с сохранённым токеном

```python
import asyncio
import os

from domofon_letai import DomofonLetaiClient


async def main() -> None:
    async with DomofonLetaiClient(
        "79000000000",
        access_token=os.environ["DOMOFON_LETAI_TOKEN"],
    ) as client:
        intercoms = await client.list_intercoms()
        await client.open_door(intercoms[0].id)


asyncio.run(main())
```

Успешный ответ API означает, что команда открытия принята, а не подтверждает физическое
состояние двери.

## Входящие звонки

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

        async with client.incoming_calls(credential_store=store) as calls:
            async for event in calls:
                intercom = by_login.get(event.sip_login)
                print("Звонок:", intercom.name if intercom else event.sip_login)

                # Здесь можно запустить распознавание лица, получить свежий кадр,
                # отправить уведомление или после своей проверки открыть дверь.


asyncio.run(main())
```

Credentials FCM обязательно сохраняются между запусками. Встроенное файловое хранилище
использует атомарную запись и права `0600`, но не шифрует файл. Push подтверждает начало
звонка, но не сообщает его достоверное окончание и не содержит полного SIP INVITE.

Подробности, модель события, безопасность и собственное хранилище:
[`docs/incoming-calls.md`](docs/incoming-calls.md). Полный список API:
[`docs/api.md`](docs/api.md).

## Видео

У домофона обычно доступны два источника:

- `StreamFormat.HLS` — проще воспроизводить, но задержка часто составляет несколько
  сегментов;
- `StreamFormat.MPEG_TS` — предпочтительный вариант для распознавания лиц и других
  low-latency задач.

URL считаются временными и непрозрачными. Не собирайте их вручную — запрашивайте через
`get_stream_source()` или `open_stream()`.

### Сохранение MPEG-TS

```python
import asyncio
import os

from domofon_letai import DomofonLetaiClient


async def main() -> None:
    async with DomofonLetaiClient(
        os.environ["DOMOFON_LETAI_PHONE"],
        access_token=os.environ["DOMOFON_LETAI_TOKEN"],
    ) as client:
        intercom = (await client.list_intercoms())[0]

        async with client.open_stream(intercom.id) as stream:
            with open("camera.ts", "wb") as output:
                async for chunk in stream.aiter_bytes():
                    output.write(chunk)


asyncio.run(main())
```

`open_stream()` по умолчанию открывает MPEG-TS. Он не скачивает поток целиком и не
создаёт внутреннюю очередь кадров. Декодирование выполняется отдельно через FFmpeg,
PyAV, GStreamer или другой MPEG-TS/H.264 decoder.

Для распознавания лиц декодирующий поток должен постоянно обновлять один слот
«последний кадр». Не складывайте все кадры в неограниченную очередь: если модель
медленнее камеры, такая очередь будет постоянно увеличивать задержку.

### TLS медиасервера

Проверка TLS включена по умолчанию. Если медиасервер отдаёт неполную цепочку
сертификатов, временно можно создать клиент так:

```python
client = DomofonLetaiClient(
    phone,
    access_token=token,
    media_verify=False,
)
```

Это снижает безопасность и делает media-соединение уязвимым для MITM. Предпочтительнее
передать корректный `ssl.SSLContext` с доверенным CA.

## Публичный API

```python
DomofonLetaiClient(...)
await client.request_sms_code()
token = await client.confirm_sms_code(code)
intercoms = await client.list_intercoms()
intercom = await client.get_intercom(intercom_id)
await client.open_door(intercom_id)
sip = await client.get_sip_settings()
async with client.incoming_calls(credential_store=store) as calls: ...
source = await client.get_stream_source(intercom_id)
async with client.open_stream(intercom_id) as stream: ...
await client.aclose()
```

Основные модели: `Intercom`, `Building`, `StreamSource`, `StreamFormat`, `MediaStream`,
`IncomingCallEvent` и `SipSettings`. Все библиотечные ошибки наследуются от
`DomofonLetaiError`.

## Известные ограничения

- API неофициальный и не имеет гарантии обратной совместимости.
- Refresh-token flow неизвестен; после `AuthenticationError` нужен новый явный вход по
  SMS.
- Поддерживаются уведомления о начале входящего звонка; управление SIP-диалогом,
  аудио/RTP и подтверждённое событие окончания звонка пока не реализованы.
- Задержку, уже добавленную камерой или медиасервером оператора, клиент устранить не
  может.
- Запрос открытия двери намеренно не повторяется автоматически.

## Разработка

```bash
python -m pip install -e ".[test,calls]"
ruff check .
mypy
pytest -q
python -m build
python -m twine check dist/*
```

CI проверяет Python 3.10–3.13, линтер, строгую типизацию, тесты и сборку wheel/sdist.
