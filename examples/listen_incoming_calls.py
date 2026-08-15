"""Listen for incoming intercom calls and run custom application logic."""

import asyncio
import os
from contextlib import suppress
from pathlib import Path

from domofon_letai import DomofonLetaiClient, FileFcmCredentialStore


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
        ) as calls:
            async for event in calls:
                intercom = by_sip_login.get(event.sip_login)
                print(
                    "Incoming call:",
                    intercom.name if intercom else event.sip_login,
                    event.call_id,
                )

                # Put face recognition, notifications, or other idempotent logic here.
                # Do not open the door automatically without an explicit security rule.


if __name__ == "__main__":
    with suppress(KeyboardInterrupt):
        asyncio.run(main())
