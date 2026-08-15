"""Request an SMS code and print intercoms after authentication."""

import asyncio
import getpass
import os

from domofon_letai import DomofonLetaiClient


async def main() -> None:
    phone = os.environ.get("DOMOFON_LETAI_PHONE") or input("Phone: ")

    async with DomofonLetaiClient(phone) as client:
        await client.request_sms_code()
        token = await client.confirm_sms_code(getpass.getpass("SMS code: "))
        print("Store this token securely:", token)

        for intercom in await client.list_intercoms():
            print(intercom.id, intercom.name, intercom.addresses)


if __name__ == "__main__":
    asyncio.run(main())
