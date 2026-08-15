"""Public value objects returned by the client."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class StreamFormat(str, Enum):
    """Video formats exposed by the Tattelecom media server."""

    HLS = "hls"
    MPEG_TS = "mpeg_ts"


@dataclass(frozen=True, slots=True)
class Building:
    """A building associated with an intercom panel."""

    id: int
    address: str


@dataclass(frozen=True, slots=True)
class StreamSource:
    """An opaque media URL obtained from the API."""

    intercom_id: int
    format: StreamFormat
    url: str = field(repr=False)


@dataclass(frozen=True, slots=True)
class Intercom:
    """A single intercom panel available to the subscriber."""

    id: int
    name: str
    sip_login: str | None
    muted: bool
    buildings: tuple[Building, ...]
    hls: StreamSource | None
    mpeg_ts: StreamSource | None
    extra: Mapping[str, Any] = field(repr=False, compare=False)

    @property
    def addresses(self) -> tuple[str, ...]:
        """Return every non-empty building address."""

        return tuple(
            building.address for building in self.buildings if building.address
        )

    @property
    def address(self) -> str:
        """Return the first building address, or an empty string."""

        return self.addresses[0] if self.addresses else ""
