"""Open the door after an application decision and dismiss the SIP call."""

import asyncio
import os
from contextlib import suppress
from pathlib import Path

from domofon_letai import DomofonLetaiClient, FileFcmCredentialStore


async def is_authorized_visitor(_intercom_id: int) -> bool:
    """Replace this stub with an explicit recognition or authorization rule."""
    return False


async def main() -> None:
    credential_store = FileFcmCredentialStore(
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

        async with client.incoming_calls(
            credential_store=credential_store
        ) as pushes:
            async for event in pushes:
                intercom = by_sip_login.get(event.sip_login)
                if intercom is None:
                    continue

                call = await client.connect_incoming_call(event)
                async with call:
                    if await is_authorized_visitor(intercom.id):
                        await call.open_door_and_end(intercom.id)
                    else:
                        await call.decline()


if __name__ == "__main__":
    with suppress(KeyboardInterrupt):
        asyncio.run(main())
