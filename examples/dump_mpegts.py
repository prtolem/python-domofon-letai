"""Write a low-latency MPEG-TS stream to stdout or a file."""

import argparse
import asyncio
import os
import sys
from contextlib import ExitStack

from domofon_letai import DomofonLetaiClient


async def dump(intercom_id: int, output_path: str | None) -> None:
    async with DomofonLetaiClient(
        os.environ["DOMOFON_LETAI_PHONE"],
        access_token=os.environ["DOMOFON_LETAI_TOKEN"],
    ) as client:
        with ExitStack() as stack:
            output = (
                stack.enter_context(open(output_path, "wb"))
                if output_path
                else sys.stdout.buffer
            )
            async with client.open_stream(intercom_id) as stream:
                async for chunk in stream.aiter_bytes():
                    output.write(chunk)
                    output.flush()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("intercom_id", type=int)
    parser.add_argument("--output")
    arguments = parser.parse_args()
    asyncio.run(dump(arguments.intercom_id, arguments.output))
