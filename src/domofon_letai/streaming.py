"""Streaming primitives exposed by the library."""

from __future__ import annotations

from collections.abc import AsyncIterator, Mapping

import httpx


class MediaStream:
    """A non-buffering view over an open media response.

    Instances are created by ``DomofonLetaiClient.open_stream`` and are valid only
    inside that async context manager.
    """

    __slots__ = ("_response",)

    def __init__(self, response: httpx.Response) -> None:
        self._response = response

    @property
    def url(self) -> str:
        """Final media URL used for the request."""

        return str(self._response.url)

    @property
    def status_code(self) -> int:
        """HTTP status returned by the media server."""

        return self._response.status_code

    @property
    def content_type(self) -> str | None:
        """Media content type, when supplied by the server."""

        value = self._response.headers.get("content-type")
        return str(value) if value is not None else None

    @property
    def headers(self) -> Mapping[str, str]:
        """Read-only response headers."""

        return self._response.headers

    async def aiter_bytes(self, chunk_size: int | None = None) -> AsyncIterator[bytes]:
        """Yield raw network data without collecting the stream in memory."""

        async for chunk in self._response.aiter_raw(chunk_size=chunk_size):
            yield chunk
