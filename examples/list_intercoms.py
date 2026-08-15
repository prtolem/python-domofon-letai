"""List intercoms using a previously stored access token."""

import asyncio
import os

from domofon_letai import DomofonLetaiClient


async def main() -> None:
    async with DomofonLetaiClient(
        os.environ["DOMOFON_LETAI_PHONE"],
        access_token=os.environ["DOMOFON_LETAI_TOKEN"],
    ) as client:
        for intercom in await client.list_intercoms():
            print(
                f"[{intercom.id}] {intercom.name}: {intercom.address or '-'} "
                f"mpeg_ts={intercom.mpeg_ts is not None} hls={intercom.hls is not None}"
            )


if __name__ == "__main__":
    asyncio.run(main())
