"""Framework-independent asynchronous SIP-over-TLS signaling primitives."""

from __future__ import annotations

import asyncio
import hashlib
import ipaddress
import re
import secrets
import ssl
from collections.abc import Awaitable, Iterable
from dataclasses import dataclass
from typing import Final, TypeAlias, TypeVar, cast, final

from .exceptions import (
    SipAuthenticationError,
    SipProtocolError,
    SipTimeoutError,
    SipTransportError,
)

__all__ = (
    "ByeBuilder",
    "DialogTags",
    "DigestChallenge",
    "Header",
    "RegisterBuilder",
    "SipMessage",
    "SipResponseBuilder",
    "SipStreamReader",
    "SipTlsConnection",
    "TransactionKey",
    "build_bye",
    "build_decline_response",
    "build_inactive_sdp_answer",
    "build_ok_response",
    "build_request_terminated_response",
    "build_response",
    "build_ringing_response",
    "build_trying_response",
    "dialog_tags",
    "ensure_header_tag",
    "parse_digest_challenge",
    "parse_header_tag",
    "parse_sip_uri",
    "top_via_branch",
    "transaction_key",
)

DialogTags: TypeAlias = tuple[str, str]
Header: TypeAlias = tuple[str, str]
TransactionKey: TypeAlias = tuple[str, int, str, str, str]
_T = TypeVar("_T")

_CRLF: Final = b"\r\n"
_HEADER_END: Final = b"\r\n\r\n"
_HEADER_NAME_RE: Final = re.compile(r"^[!#$%&'*+\-.^_`|~0-9A-Za-z]+$")
_RESPONSE_LINE_RE: Final = re.compile(r"^SIP/2\.0[ \t]+([0-9]{3})(?:[ \t]|$)")
_CSEQ_RE: Final = re.compile(r"^[ \t]*([0-9]+)[ \t]+([^ \t]+)[ \t]*$")
_TAG_RE: Final = re.compile(
    r"(?:^|;)[ \t]*tag[ \t]*=[ \t]*(?:\"((?:\\.|[^\"])*)\"|([^;, \t]+))",
    re.IGNORECASE,
)
_COMPACT_HEADERS: Final[dict[str, frozenset[str]]] = {
    "call-id": frozenset(("call-id", "i")),
    "contact": frozenset(("contact", "m")),
    "content-length": frozenset(("content-length", "l")),
    "from": frozenset(("from", "f")),
    "to": frozenset(("to", "t")),
    "via": frozenset(("via", "v")),
}


def _validate_line(value: str, description: str) -> None:
    if not value or "\r" in value or "\n" in value:
        raise SipProtocolError(f"Invalid SIP {description}")


def _header_names(name: str) -> frozenset[str]:
    folded = name.casefold()
    return _COMPACT_HEADERS.get(folded, frozenset((folded,)))


@final
@dataclass(frozen=True, slots=True, init=False)
class SipMessage:
    """A SIP start line, ordered headers, and an opaque byte body."""

    start_line: str
    headers: tuple[Header, ...]
    body: bytes

    def __init__(
        self,
        start_line: str,
        headers: Iterable[Header] = (),
        body: bytes = b"",
    ) -> None:
        """Create a message while defensively freezing its headers and body."""
        _validate_line(start_line, "start line")
        normalized: list[Header] = []
        for name, value in headers:
            if not _HEADER_NAME_RE.fullmatch(name):
                raise SipProtocolError("Invalid SIP header name")
            if "\r" in value or "\n" in value:
                raise SipProtocolError(f"Invalid value for SIP header {name}")
            normalized.append((name, value))

        object.__setattr__(self, "start_line", start_line)
        object.__setattr__(self, "headers", tuple(normalized))
        object.__setattr__(self, "body", bytes(body))

    def get_headers(self, name: str) -> tuple[str, ...]:
        """Return all values for a header using case-insensitive matching."""
        names = _header_names(name)
        return tuple(value for key, value in self.headers if key.casefold() in names)

    def get_header(self, name: str) -> str | None:
        """Return the first matching header value, if present."""
        values = self.get_headers(name)
        return values[0] if values else None

    @property
    def method(self) -> str | None:
        """Return the request method, or ``None`` for a response."""
        if self.start_line.casefold().startswith("sip/2.0"):
            return None
        method, separator, _remainder = self.start_line.partition(" ")
        return method if method and separator else None

    @property
    def status_code(self) -> int | None:
        """Return the response status code, or ``None`` for a request."""
        match = _RESPONSE_LINE_RE.match(self.start_line)
        return int(match.group(1)) if match is not None else None

    @property
    def call_id(self) -> str | None:
        """Return the Call-ID value, including support for compact ``i``."""
        return self.get_header("Call-ID")

    def _cseq_parts(self) -> tuple[int, str] | None:
        value = self.get_header("CSeq")
        if value is None:
            return None
        match = _CSEQ_RE.fullmatch(value)
        if match is None:
            return None
        return int(match.group(1)), match.group(2)

    @property
    def cseq_method(self) -> str | None:
        """Return the CSeq method when the CSeq header is valid."""
        parts = self._cseq_parts()
        return parts[1] if parts is not None else None

    @property
    def cseq_number(self) -> int | None:
        """Return the CSeq number when the CSeq header is valid."""
        parts = self._cseq_parts()
        return parts[0] if parts is not None else None

    def to_bytes(self) -> bytes:
        """Serialize the message without altering header order or values."""
        lines = [self.start_line, *(f"{name}: {value}" for name, value in self.headers)]
        return "\r\n".join(lines).encode("utf-8") + _HEADER_END + self.body

    def __bytes__(self) -> bytes:
        """Return :meth:`to_bytes`."""
        return self.to_bytes()


@final
class SipStreamReader:
    """Frame SIP messages from an ``asyncio.StreamReader``."""

    DEFAULT_HEADER_LIMIT: Final = 64 * 1024
    DEFAULT_BODY_LIMIT: Final = 1024 * 1024

    def __init__(
        self,
        reader: asyncio.StreamReader,
        *,
        timeout: float | None = None,
        header_limit: int = DEFAULT_HEADER_LIMIT,
        body_limit: int = DEFAULT_BODY_LIMIT,
    ) -> None:
        """Configure framing limits and a per-read timeout."""
        if timeout is not None and timeout <= 0:
            raise ValueError("timeout must be positive or None")
        if header_limit <= 0 or body_limit < 0:
            raise ValueError("SIP framing limits must be positive")
        self._reader = reader
        self._timeout = timeout
        self._header_limit = header_limit
        self._body_limit = body_limit

    async def _wait(self, awaitable: Awaitable[_T]) -> _T:
        if self._timeout is None:
            return await awaitable
        return await asyncio.wait_for(awaitable, timeout=self._timeout)

    async def _read_header_block(self) -> bytes:
        while True:
            try:
                framed = await self._wait(self._reader.readuntil(_HEADER_END))
            except asyncio.LimitOverrunError as error:
                raise SipProtocolError("SIP header exceeds configured limit") from error
            except asyncio.IncompleteReadError as error:
                raise SipTransportError("SIP stream closed during a header") from error

            while framed.startswith(_CRLF):
                framed = framed[len(_CRLF) :]
            if not framed:
                continue
            if len(framed) < len(_HEADER_END) or not framed.endswith(_HEADER_END):
                raise SipProtocolError("Malformed SIP header delimiter")

            block = framed[: -len(_HEADER_END)]
            if len(block) > self._header_limit:
                raise SipProtocolError("SIP header exceeds configured limit")
            return block

    @staticmethod
    def _parse_headers(block: bytes) -> tuple[str, tuple[Header, ...]]:
        try:
            text = block.decode("utf-8")
        except UnicodeDecodeError as error:
            raise SipProtocolError("SIP headers are not valid UTF-8") from error

        lines = text.split("\r\n")
        if not lines or not lines[0]:
            raise SipProtocolError("SIP message has no start line")
        start_line = lines[0]
        parsed: list[Header] = []
        for line in lines[1:]:
            if line.startswith((" ", "\t")):
                if not parsed:
                    raise SipProtocolError("SIP header continuation has no field")
                name, previous = parsed[-1]
                parsed[-1] = (name, f"{previous} {line.strip()}")
                continue
            name, separator, value = line.partition(":")
            if not separator or not _HEADER_NAME_RE.fullmatch(name):
                raise SipProtocolError("Malformed SIP header")
            parsed.append((name, value.strip(" \t")))
        return start_line, tuple(parsed)

    @staticmethod
    def _content_length(headers: tuple[Header, ...]) -> int:
        names = _header_names("Content-Length")
        values = [value.strip() for name, value in headers if name.casefold() in names]
        if not values:
            return 0
        lengths: list[int] = []
        for value in values:
            if not value or not value.isascii() or not value.isdecimal():
                raise SipProtocolError("Invalid SIP Content-Length")
            lengths.append(int(value))
        if len(set(lengths)) != 1:
            raise SipProtocolError("Conflicting SIP Content-Length headers")
        return lengths[0]

    async def read_message(self) -> SipMessage:
        """Read one SIP message, silently consuming CRLF keepalives."""
        try:
            block = await self._read_header_block()
            start_line, headers = self._parse_headers(block)
            content_length = self._content_length(headers)
            if content_length > self._body_limit:
                raise SipProtocolError("SIP body exceeds configured limit")
            try:
                body = await self._wait(self._reader.readexactly(content_length))
            except asyncio.IncompleteReadError as error:
                raise SipTransportError("SIP stream closed during a body") from error
        except asyncio.TimeoutError as error:
            raise SipTimeoutError("Timed out while receiving a SIP message") from error
        return SipMessage(start_line, headers, body)


@final
@dataclass(frozen=True, slots=True)
class DigestChallenge:
    """Parsed SIP Digest authentication challenge."""

    realm: str
    nonce: str
    algorithm: str
    qop: str | None = None
    opaque: str | None = None


def _split_quoted(value: str) -> tuple[str, ...]:
    parts: list[str] = []
    current: list[str] = []
    quoted = False
    escaped = False
    for character in value:
        if escaped:
            current.append(character)
            escaped = False
        elif character == "\\" and quoted:
            current.append(character)
            escaped = True
        elif character == '"':
            current.append(character)
            quoted = not quoted
        elif character == "," and not quoted:
            parts.append("".join(current).strip())
            current.clear()
        else:
            current.append(character)
    if quoted or escaped:
        raise SipAuthenticationError("Malformed quoted Digest challenge")
    parts.append("".join(current).strip())
    return tuple(part for part in parts if part)


def _unquote_digest(value: str) -> str:
    if not value.startswith('"'):
        return value
    if len(value) < 2 or not value.endswith('"'):
        raise SipAuthenticationError("Malformed Digest challenge value")
    result: list[str] = []
    escaped = False
    for character in value[1:-1]:
        if escaped:
            result.append(character)
            escaped = False
        elif character == "\\":
            escaped = True
        else:
            result.append(character)
    if escaped:
        raise SipAuthenticationError("Malformed Digest challenge escape")
    return "".join(result)


def _parse_digest_value(value: str) -> DigestChallenge:
    scheme, separator, parameters = value.partition(" ")
    if not separator or scheme.casefold() != "digest":
        raise SipAuthenticationError("Unsupported SIP authentication scheme")
    parsed: dict[str, str] = {}
    for item in _split_quoted(parameters):
        key, equals, raw_value = item.partition("=")
        if not equals or not key.strip() or not raw_value.strip():
            raise SipAuthenticationError("Malformed Digest challenge parameter")
        parsed[key.strip().casefold()] = _unquote_digest(raw_value.strip())

    realm = parsed.get("realm")
    nonce = parsed.get("nonce")
    if realm is None or nonce is None:
        raise SipAuthenticationError("Digest challenge lacks realm or nonce")
    algorithm = parsed.get("algorithm", "MD5").upper()
    if algorithm not in {"MD5", "SHA-256"}:
        raise SipAuthenticationError(f"Unsupported Digest algorithm: {algorithm}")
    return DigestChallenge(
        realm=realm,
        nonce=nonce,
        algorithm=algorithm,
        qop=parsed.get("qop"),
        opaque=parsed.get("opaque"),
    )


def parse_digest_challenge(response: SipMessage) -> DigestChallenge:
    """Parse a 401 WWW or 407 Proxy Digest challenge."""
    if response.status_code == 401:
        header_name = "WWW-Authenticate"
    elif response.status_code == 407:
        header_name = "Proxy-Authenticate"
    else:
        raise SipAuthenticationError("Expected SIP status 401 or 407")

    errors: list[SipAuthenticationError] = []
    for value in response.get_headers(header_name):
        try:
            return _parse_digest_value(value)
        except SipAuthenticationError as error:
            errors.append(error)
    if errors:
        raise errors[-1]
    raise SipAuthenticationError(f"SIP response lacks {header_name}")


def _format_host(host: str) -> str:
    if host.startswith("[") and host.endswith("]"):
        return host
    return f"[{host}]" if ":" in host else host


def _host_port(host: str, port: int) -> str:
    if not 1 <= port <= 65535:
        raise ValueError("SIP port must be between 1 and 65535")
    return f"{_format_host(host)}:{port}"


def _new_branch() -> str:
    return f"z9hG4bK.{secrets.token_hex(8)}"


def _new_tag() -> str:
    return secrets.token_hex(8)


def _quote_digest(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"')


def _digest_hex(algorithm: str, value: str) -> str:
    encoded = value.encode("utf-8")
    if algorithm == "MD5":
        return hashlib.md5(encoded, usedforsecurity=False).hexdigest()
    return hashlib.sha256(encoded).hexdigest()


@final
class RegisterBuilder:
    """Build a sequence of TLS REGISTER requests with optional Digest auth."""

    def __init__(
        self,
        username: str,
        password: str,
        registrar_host: str,
        registrar_port: int,
        local_host: str,
        local_port: int,
        *,
        domain: str | None = None,
        expires: int = 3600,
        user_agent: str = "domofon-letai",
        call_id: str | None = None,
        from_tag: str | None = None,
        initial_cseq: int = 1,
    ) -> None:
        """Initialize stable registration identity and mutable CSeq state."""
        for value, description in (
            (username, "username"),
            (registrar_host, "registrar host"),
            (local_host, "local host"),
            (domain or registrar_host, "domain"),
            (user_agent, "user agent"),
        ):
            _validate_line(value, description)
        if expires < 0:
            raise ValueError("expires must not be negative")
        if initial_cseq < 0:
            raise ValueError("initial_cseq must not be negative")

        self._username = username
        self._password = password
        self._registrar_host = registrar_host
        self._registrar_port = registrar_port
        self._local_host = local_host
        self._local_port = local_port
        self._domain = domain or registrar_host
        self._expires = expires
        self._user_agent = user_agent
        self._call_id = call_id or f"{secrets.token_hex(16)}@{local_host}"
        self._from_tag = from_tag or _new_tag()
        self._next_cseq = initial_cseq
        self._nonce_counts: dict[str, int] = {}

    @property
    def call_id(self) -> str:
        """Return the stable registration Call-ID."""
        return self._call_id

    @property
    def from_tag(self) -> str:
        """Return the stable registration From tag."""
        return self._from_tag

    @property
    def next_cseq(self) -> int:
        """Return the CSeq that the next successful build will use."""
        return self._next_cseq

    @property
    def request_uri(self) -> str:
        """Return the TLS registrar request URI."""
        registrar = _host_port(self._registrar_host, self._registrar_port)
        return f"sip:{registrar};transport=tls"

    def _authorization(self, response: SipMessage) -> Header:
        challenge = parse_digest_challenge(response)
        qop: str | None = None
        if challenge.qop is not None:
            options = tuple(
                option.strip().casefold()
                for option in challenge.qop.split(",")
                if option.strip()
            )
            if "auth" not in options:
                raise SipAuthenticationError("Digest challenge does not offer qop=auth")
            qop = "auth"

        ha1 = _digest_hex(
            challenge.algorithm,
            f"{self._username}:{challenge.realm}:{self._password}",
        )
        ha2 = _digest_hex(challenge.algorithm, f"REGISTER:{self.request_uri}")
        directives = [
            f'username="{_quote_digest(self._username)}"',
            f'realm="{_quote_digest(challenge.realm)}"',
            f'nonce="{_quote_digest(challenge.nonce)}"',
            f'uri="{_quote_digest(self.request_uri)}"',
        ]
        if qop is None:
            digest = _digest_hex(
                challenge.algorithm,
                f"{ha1}:{challenge.nonce}:{ha2}",
            )
        else:
            nonce_count = self._nonce_counts.get(challenge.nonce, 0) + 1
            self._nonce_counts[challenge.nonce] = nonce_count
            nc_value = f"{nonce_count:08x}"
            cnonce = secrets.token_hex(16)
            digest = _digest_hex(
                challenge.algorithm,
                f"{ha1}:{challenge.nonce}:{nc_value}:{cnonce}:{qop}:{ha2}",
            )
            directives.extend((f"qop={qop}", f"nc={nc_value}", f'cnonce="{cnonce}"'))
        directives.append(f'response="{digest}"')
        directives.append(f"algorithm={challenge.algorithm}")
        if challenge.opaque is not None:
            directives.append(f'opaque="{_quote_digest(challenge.opaque)}"')

        name = "Authorization" if response.status_code == 401 else "Proxy-Authorization"
        return name, f"Digest {','.join(directives)}"

    def _build_register(
        self,
        challenge: SipMessage | None,
        *,
        expires: int,
        contact_expires: int | None,
    ) -> SipMessage:
        cseq = self._next_cseq
        local = _host_port(self._local_host, self._local_port)
        aor = f"sip:{self._username}@{_format_host(self._domain)}"
        contact = f"<sip:{self._username}@{local};transport=tls>"
        if contact_expires is not None:
            contact = f"{contact};expires={contact_expires}"
        headers: list[Header] = [
            ("Via", f"SIP/2.0/TLS {local};branch={_new_branch()};rport"),
            ("Max-Forwards", "70"),
            ("From", f"<{aor}>;tag={self._from_tag}"),
            ("To", f"<{aor}>"),
            ("Call-ID", self._call_id),
            ("CSeq", f"{cseq} REGISTER"),
            ("Contact", contact),
            ("Expires", str(expires)),
            ("User-Agent", self._user_agent),
        ]
        if challenge is not None:
            headers.append(self._authorization(challenge))
        headers.append(("Content-Length", "0"))
        message = SipMessage(f"REGISTER {self.request_uri} SIP/2.0", headers)
        self._next_cseq += 1
        return message

    def build(self, challenge: SipMessage | None = None) -> SipMessage:
        """Build the next REGISTER and advance CSeq after successful creation."""
        return self._build_register(
            challenge,
            expires=self._expires,
            contact_expires=None,
        )

    def build_deregister(self, challenge: SipMessage | None = None) -> SipMessage:
        """Build a zero-expiry REGISTER using the next CSeq."""
        return self._build_register(
            challenge,
            expires=0,
            contact_expires=0,
        )

    def build_initial(self) -> SipMessage:
        """Build an unauthenticated REGISTER."""
        return self.build()

    def build_authenticated(self, challenge: SipMessage) -> SipMessage:
        """Build a REGISTER authenticated from a 401 or 407 response."""
        return self.build(challenge)


def _split_header_values(value: str, description: str) -> tuple[str, ...]:
    _validate_line(value, description)
    values: list[str] = []
    current: list[str] = []
    angle_depth = 0
    quoted = False
    escaped = False
    for character in value:
        if escaped:
            current.append(character)
            escaped = False
        elif quoted and character == "\\":
            current.append(character)
            escaped = True
        elif character == '"':
            current.append(character)
            quoted = not quoted
        elif not quoted and character == "<":
            if angle_depth != 0:
                raise SipProtocolError(f"Malformed {description} angle brackets")
            angle_depth = 1
            current.append(character)
        elif not quoted and character == ">":
            if angle_depth != 1:
                raise SipProtocolError(f"Malformed {description} angle brackets")
            angle_depth = 0
            current.append(character)
        elif not quoted and angle_depth == 0 and character == ",":
            item = "".join(current).strip()
            if not item:
                raise SipProtocolError(f"Malformed comma-separated {description}")
            values.append(item)
            current.clear()
        else:
            current.append(character)
    if quoted or escaped or angle_depth != 0:
        raise SipProtocolError(f"Malformed {description}")
    item = "".join(current).strip()
    if not item:
        raise SipProtocolError(f"Malformed comma-separated {description}")
    values.append(item)
    return tuple(values)


def parse_sip_uri(header_value: str) -> str:
    """Extract the first SIP or SIPS URI from a name-address header value."""
    first_value = _split_header_values(header_value, "address header")[0]
    start = first_value.find("<")
    if start >= 0:
        end = first_value.find(">", start + 1)
        if end < 0:
            raise SipProtocolError("SIP name-address lacks closing bracket")
        uri = first_value[start + 1 : end].strip()
    else:
        uri = first_value
        tag_match = re.search(r";[ \t]*tag[ \t]*=", uri, re.IGNORECASE)
        if tag_match is not None:
            uri = uri[: tag_match.start()].rstrip()
    if not uri.casefold().startswith(("sip:", "sips:")):
        raise SipProtocolError("SIP address does not contain a SIP URI")
    if any(character.isspace() for character in uri):
        raise SipProtocolError("SIP URI contains whitespace")
    return uri


def _uri_has_lr(uri: str) -> bool:
    uri_without_headers = uri.split("?", maxsplit=1)[0]
    return any(
        parameter.partition("=")[0].strip().casefold() == "lr"
        for parameter in uri_without_headers.split(";")[1:]
    )


def parse_header_tag(header_value: str) -> str | None:
    """Return a SIP name-address ``tag`` parameter, if present."""
    _validate_line(header_value, "address header")
    end = header_value.find(">")
    parameters = header_value[end + 1 :] if end >= 0 else header_value
    match = _TAG_RE.search(parameters)
    if match is None:
        return None
    quoted, token = match.groups()
    if quoted is not None:
        return re.sub(r"\\(.)", r"\1", quoted)
    return token


def ensure_header_tag(header_value: str, tag: str) -> str:
    """Append ``tag`` unless the address already has one."""
    _validate_line(tag, "tag")
    if not tag or any(character in tag for character in " ;,\t"):
        raise SipProtocolError("Invalid SIP tag")
    if parse_header_tag(header_value) is not None:
        return header_value
    return f"{header_value};tag={tag}"


def _required_header(message: SipMessage, name: str) -> Header:
    names = _header_names(name)
    for actual_name, value in message.headers:
        if actual_name.casefold() in names:
            return actual_name, value
    raise SipProtocolError(f"SIP message lacks {name}")


def _strict_header_value(message: SipMessage, name: str) -> str:
    values = message.get_headers(name)
    if len(values) != 1:
        raise SipProtocolError(f"SIP message must contain exactly one {name}")
    value = values[0].strip()
    if not value:
        raise SipProtocolError(f"SIP {name} must not be empty")
    return value


def _top_via_transaction_parts(message: SipMessage) -> tuple[str, str]:
    via_headers = message.get_headers("Via")
    if not via_headers:
        raise SipProtocolError("SIP message lacks Via")
    top_via = _split_header_values(via_headers[0], "Via")[0]
    first, *parameters = top_via.split(";")
    match = re.fullmatch(
        r"SIP/2\.0/[A-Za-z]+[ \t]+(\S+)",
        first,
        re.IGNORECASE,
    )
    if match is None:
        raise SipProtocolError("Malformed top SIP Via")
    sent_by = match.group(1).casefold()

    branches: list[str] = []
    for parameter in parameters:
        name, equals, value = parameter.partition("=")
        if name.strip().casefold() != "branch":
            continue
        branch = value.strip()
        if not equals or not branch or _HEADER_NAME_RE.fullmatch(branch) is None:
            raise SipProtocolError("Malformed top SIP Via branch")
        branches.append(branch)
    if len(branches) != 1:
        raise SipProtocolError("Top SIP Via must contain exactly one branch")
    return branches[0], sent_by


def top_via_branch(message: SipMessage) -> str:
    """Return the branch parameter from the topmost Via value."""
    branch, _sent_by = _top_via_transaction_parts(message)
    return branch


def transaction_key(message: SipMessage) -> TransactionKey:
    """Return strict identifiers for matching a SIP transaction."""
    call_id = _strict_header_value(message, "Call-ID")
    cseq = _strict_header_value(message, "CSeq")
    match = _CSEQ_RE.fullmatch(cseq)
    if match is None:
        raise SipProtocolError("Malformed SIP CSeq")
    cseq_number = int(match.group(1))
    cseq_method = match.group(2)
    if cseq_number >= 2**31 or _HEADER_NAME_RE.fullmatch(cseq_method) is None:
        raise SipProtocolError("Malformed SIP CSeq")

    request_method = message.method
    status_code = message.status_code
    request_parts = message.start_line.split()
    if request_method is not None and (
        len(request_parts) != 3
        or request_parts[0] != request_method
        or request_parts[2].casefold() != "sip/2.0"
    ):
        raise SipProtocolError("Malformed SIP request line")
    if (
        request_method is not None
        and request_method.casefold() != cseq_method.casefold()
    ):
        raise SipProtocolError("SIP request method conflicts with CSeq")
    if request_method is None and (
        status_code is None or not 100 <= status_code <= 699
    ):
        raise SipProtocolError("Malformed SIP start line")
    branch, sent_by = _top_via_transaction_parts(message)
    return call_id, cseq_number, cseq_method.upper(), branch, sent_by


def dialog_tags(message: SipMessage) -> DialogTags:
    """Return mandatory ``(From tag, To tag)`` dialog identifiers."""
    from_value = _strict_header_value(message, "From")
    to_value = _strict_header_value(message, "To")
    from_tag = parse_header_tag(from_value)
    to_tag = parse_header_tag(to_value)
    if from_tag is None or not from_tag:
        raise SipProtocolError("SIP From header lacks a tag")
    if to_tag is None or not to_tag:
        raise SipProtocolError("SIP To header lacks a tag")
    return from_tag, to_tag


@final
class SipResponseBuilder:
    """Build transaction responses with a stable local To tag."""

    def __init__(self, request: SipMessage, *, local_tag: str | None = None) -> None:
        """Bind the builder to one request and local dialog tag."""
        if request.method is None:
            raise SipProtocolError("Cannot respond to a SIP response")
        if local_tag is not None:
            _validate_line(local_tag, "tag")
        _ = _required_header(request, "From")
        _to_name, to_value = _required_header(request, "To")
        _ = _required_header(request, "Call-ID")
        _ = _required_header(request, "CSeq")
        if not request.get_headers("Via"):
            raise SipProtocolError("SIP request lacks Via")

        existing_tag = parse_header_tag(to_value)
        if (
            existing_tag is not None
            and local_tag is not None
            and existing_tag != local_tag
        ):
            raise SipProtocolError("SIP To tag conflicts with the local tag")
        self._request = request
        self._local_tag = existing_tag or local_tag or _new_tag()

    @property
    def local_tag(self) -> str:
        """Return the stable local dialog tag."""
        return self._local_tag

    def build(
        self,
        status_code: int,
        reason: str,
        *,
        body: bytes = b"",
        extra_headers: Iterable[Header] = (),
    ) -> SipMessage:
        """Build a response while copying transaction headers from the request."""
        if not 100 <= status_code <= 699:
            raise ValueError("SIP status code must be between 100 and 699")
        _validate_line(reason, "reason phrase")

        headers: list[Header] = [
            header
            for header in self._request.headers
            if header[0].casefold() in _header_names("Via")
        ]
        headers.append(_required_header(self._request, "From"))
        to_name, to_value = _required_header(self._request, "To")
        if status_code != 100:
            to_value = ensure_header_tag(to_value, self._local_tag)
        headers.append((to_name, to_value))
        headers.append(_required_header(self._request, "Call-ID"))
        headers.append(_required_header(self._request, "CSeq"))

        protected = frozenset(
            name
            for canonical in ("via", "from", "to", "call-id", "content-length")
            for name in _header_names(canonical)
        ) | frozenset(("cseq",))
        for name, value in extra_headers:
            if name.casefold() in protected:
                raise SipProtocolError(f"Response extra header may not replace {name}")
            headers.append((name, value))
        headers.append(("Content-Length", str(len(body))))
        return SipMessage(f"SIP/2.0 {status_code} {reason}", headers, body)

    def trying(self) -> SipMessage:
        """Build 100 Trying without adding a To tag."""
        return self.build(100, "Trying")

    def ringing(self) -> SipMessage:
        """Build 180 Ringing."""
        return self.build(180, "Ringing")

    def decline(self) -> SipMessage:
        """Build 603 Decline."""
        return self.build(603, "Decline")

    def ok(
        self,
        *,
        body: bytes = b"",
        extra_headers: Iterable[Header] = (),
    ) -> SipMessage:
        """Build a generic 200 OK response."""
        return self.build(200, "OK", body=body, extra_headers=extra_headers)

    def request_terminated(self) -> SipMessage:
        """Build 487 Request Terminated."""
        return self.build(487, "Request Terminated")


def build_response(
    request: SipMessage,
    status_code: int,
    reason: str,
    *,
    local_tag: str | None = None,
    body: bytes = b"",
    extra_headers: Iterable[Header] = (),
) -> SipMessage:
    """Build one response; retain ``local_tag`` when building related responses."""
    return SipResponseBuilder(request, local_tag=local_tag).build(
        status_code,
        reason,
        body=body,
        extra_headers=extra_headers,
    )


def build_trying_response(request: SipMessage) -> SipMessage:
    """Build 100 Trying."""
    return SipResponseBuilder(request).trying()


def build_ringing_response(
    request: SipMessage,
    *,
    local_tag: str | None = None,
) -> SipMessage:
    """Build 180 Ringing."""
    return SipResponseBuilder(request, local_tag=local_tag).ringing()


def build_decline_response(
    request: SipMessage,
    *,
    local_tag: str | None = None,
) -> SipMessage:
    """Build 603 Decline."""
    return SipResponseBuilder(request, local_tag=local_tag).decline()


def build_ok_response(
    request: SipMessage,
    *,
    local_tag: str | None = None,
    body: bytes = b"",
    extra_headers: Iterable[Header] = (),
) -> SipMessage:
    """Build a generic 200 OK response."""
    return SipResponseBuilder(request, local_tag=local_tag).ok(
        body=body,
        extra_headers=extra_headers,
    )


def build_request_terminated_response(
    request: SipMessage,
    *,
    local_tag: str | None = None,
) -> SipMessage:
    """Build 487 Request Terminated."""
    return SipResponseBuilder(request, local_tag=local_tag).request_terminated()


def build_inactive_sdp_answer(
    offer: bytes | str,
    *,
    origin_address: str = "0.0.0.0",
    session_id: int | str | None = None,
) -> bytes:
    """Build an inactive, zero-port answer preserving offered media sections."""
    try:
        text = offer.decode("utf-8") if isinstance(offer, bytes) else offer
    except UnicodeDecodeError as error:
        raise SipProtocolError("SDP offer is not valid UTF-8") from error
    if "\x00" in text:
        raise SipProtocolError("SDP offer contains a NUL byte")

    media: list[tuple[str, str, str]] = []
    for line in text.splitlines():
        if not line:
            continue
        if len(line) < 2 or line[1] != "=":
            raise SipProtocolError("Malformed SDP line")
        if line.startswith("m="):
            parts = line[2:].split()
            if len(parts) < 4 or not all(parts[index] for index in (0, 2, 3)):
                raise SipProtocolError("Malformed SDP media description")
            if re.fullmatch(r"[0-9]+(?:/[0-9]+)?", parts[1]) is None:
                raise SipProtocolError("Malformed SDP media port")
            media.append((parts[0], parts[2], parts[3]))
    if not media:
        raise SipProtocolError("SDP offer contains no media descriptions")

    try:
        address = ipaddress.ip_address(origin_address)
        address_family = "IP6" if address.version == 6 else "IP4"
    except ValueError as error:
        raise SipProtocolError("SDP origin address must be an IP address") from error
    identifier = (
        str(session_id) if session_id is not None else str(secrets.randbits(63))
    )
    if not identifier.isascii() or not identifier.isdecimal():
        raise SipProtocolError("SDP session_id must be a decimal integer")

    answer = [
        "v=0",
        f"o=- {identifier} {identifier} IN {address_family} {origin_address}",
        "s=-",
        f"c=IN {address_family} {origin_address}",
        "t=0 0",
    ]
    for media_type, protocol, payload in media:
        answer.extend((f"m={media_type} 0 {protocol} {payload}", "a=inactive"))
    return ("\r\n".join(answer) + "\r\n").encode("utf-8")


@final
class ByeBuilder:
    """Build local in-dialog BYE requests from an incoming INVITE."""

    def __init__(
        self,
        invite: SipMessage,
        local_host: str,
        local_port: int,
        *,
        local_tag: str | None = None,
        initial_cseq: int = 1,
        user_agent: str = "domofon-letai",
    ) -> None:
        """Capture dialog identifiers and an independent local CSeq."""
        if invite.method is None or invite.method.casefold() != "invite":
            raise SipProtocolError("BYE builder requires an INVITE request")
        if initial_cseq < 0:
            raise ValueError("initial_cseq must not be negative")
        _validate_line(local_host, "local host")
        _validate_line(user_agent, "user agent")

        contact = invite.get_header("Contact")
        if contact is None:
            raise SipProtocolError("INVITE lacks a Contact remote target")
        remote_target = parse_sip_uri(contact)
        _from_name, remote_identity = _required_header(invite, "From")
        if parse_header_tag(remote_identity) is None:
            raise SipProtocolError("INVITE From header lacks a remote tag")
        _to_name, local_identity = _required_header(invite, "To")
        existing_local_tag = parse_header_tag(local_identity)
        if (
            existing_local_tag is not None
            and local_tag is not None
            and existing_local_tag != local_tag
        ):
            raise SipProtocolError("INVITE To tag conflicts with the local tag")

        call_id = invite.call_id
        if call_id is None:
            raise SipProtocolError("INVITE lacks Call-ID")

        route_values: list[str] = []
        for record_route in invite.get_headers("Record-Route"):
            route_values.extend(
                _split_header_values(record_route, "Record-Route header")
            )
        route_set = tuple(route_values)
        request_uri = remote_target
        route_headers = route_set
        strict_routing = False
        if route_set:
            first_route_uri = parse_sip_uri(route_set[0])
            if not _uri_has_lr(first_route_uri):
                strict_routing = True
                request_uri = first_route_uri
                route_headers = (*route_set[1:], f"<{remote_target}>")

        self._remote_target = remote_target
        self._route_set = route_set
        self._request_uri = request_uri
        self._route_headers = route_headers
        self._strict_routing = strict_routing
        self._remote_identity = remote_identity
        self._local_identity = ensure_header_tag(
            local_identity,
            existing_local_tag or local_tag or _new_tag(),
        )
        self._call_id = call_id
        self._local_host = local_host
        self._local_port = local_port
        self._next_cseq = initial_cseq
        self._user_agent = user_agent

    @property
    def remote_target(self) -> str:
        """Return the remote target extracted from INVITE Contact."""
        return self._remote_target

    @property
    def route_set(self) -> tuple[str, ...]:
        """Return the UAS route set in incoming request order."""
        return self._route_set

    @property
    def request_uri(self) -> str:
        """Return the BYE Request-URI after strict-routing processing."""
        return self._request_uri

    @property
    def route_headers(self) -> tuple[str, ...]:
        """Return the Route values emitted by the BYE request."""
        return self._route_headers

    @property
    def strict_routing(self) -> bool:
        """Return whether the first route requires strict routing."""
        return self._strict_routing

    @property
    def local_tag(self) -> str:
        """Return the local dialog tag used in BYE From."""
        tag = parse_header_tag(self._local_identity)
        if tag is None:
            raise SipProtocolError("Internal BYE identity lacks a local tag")
        return tag

    @property
    def next_cseq(self) -> int:
        """Return the independent local CSeq for the next BYE."""
        return self._next_cseq

    def build(self) -> SipMessage:
        """Build the next TLS BYE and advance its independent CSeq."""
        local = _host_port(self._local_host, self._local_port)
        cseq = self._next_cseq
        headers: list[Header] = [
            ("Via", f"SIP/2.0/TLS {local};branch={_new_branch()};rport"),
            ("Max-Forwards", "70"),
            ("From", self._local_identity),
            ("To", self._remote_identity),
            ("Call-ID", self._call_id),
            ("CSeq", f"{cseq} BYE"),
        ]
        headers.extend(("Route", value) for value in self._route_headers)
        headers.extend(
            (
                ("User-Agent", self._user_agent),
                ("Content-Length", "0"),
            )
        )
        message = SipMessage(
            f"BYE {self._request_uri} SIP/2.0",
            headers,
        )
        self._next_cseq += 1
        return message


def build_bye(
    invite: SipMessage,
    local_host: str,
    local_port: int,
    *,
    local_tag: str | None = None,
    cseq: int = 1,
    user_agent: str = "domofon-letai",
) -> SipMessage:
    """Build one BYE request from an incoming INVITE."""
    return ByeBuilder(
        invite,
        local_host,
        local_port,
        local_tag=local_tag,
        initial_cseq=cseq,
        user_agent=user_agent,
    ).build()


@final
class SipTlsConnection:
    """Serialized writes and framed reads over one caller-configured TLS stream."""

    def __init__(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        *,
        timeout: float | None,
        local_host: str,
        local_port: int,
    ) -> None:
        self._reader = SipStreamReader(reader, timeout=timeout)
        self._writer = writer
        self._timeout = timeout
        self._write_lock = asyncio.Lock()
        self._local_host = local_host
        self._local_port = local_port

    @classmethod
    async def connect(
        cls,
        host: str,
        port: int,
        ssl_context: ssl.SSLContext,
        timeout: float | None = 10.0,
    ) -> SipTlsConnection:
        """Connect with caller-provided TLS verification and host as SNI."""
        if timeout is not None and timeout <= 0:
            raise ValueError("timeout must be positive or None")
        try:
            connection = asyncio.open_connection(
                host,
                port,
                ssl=ssl_context,
                server_hostname=host,
                limit=SipStreamReader.DEFAULT_HEADER_LIMIT + len(_HEADER_END),
            )
            if timeout is None:
                reader, writer = await connection
            else:
                reader, writer = await asyncio.wait_for(connection, timeout=timeout)
        except asyncio.TimeoutError as error:
            raise SipTimeoutError("Timed out connecting the SIP TLS stream") from error
        except OSError as error:
            raise SipTransportError("Could not connect the SIP TLS stream") from error

        sockname = cast(
            tuple[object, ...] | None,
            writer.get_extra_info("sockname"),
        )
        if not isinstance(sockname, tuple) or len(sockname) < 2:
            writer.close()
            await writer.wait_closed()
            raise SipTransportError("SIP TLS stream has no local endpoint")
        local_host = sockname[0]
        local_port = sockname[1]
        if not isinstance(local_host, str) or not isinstance(local_port, int):
            writer.close()
            await writer.wait_closed()
            raise SipTransportError("SIP TLS stream has an invalid local endpoint")
        return cls(
            reader,
            writer,
            timeout=timeout,
            local_host=local_host,
            local_port=local_port,
        )

    @property
    def local_host(self) -> str:
        """Return the local socket host selected by the operating system."""
        return self._local_host

    @property
    def local_port(self) -> int:
        """Return the local socket port selected by the operating system."""
        return self._local_port

    @property
    def is_closing(self) -> bool:
        """Return whether the underlying writer is closing."""
        return self._writer.is_closing()

    async def send(self, message: SipMessage | bytes) -> None:
        """Serialize one message or send already-built bytes under a writer lock."""
        payload = message.to_bytes() if isinstance(message, SipMessage) else message
        async with self._write_lock:
            if self._writer.is_closing():
                raise SipTransportError("SIP TLS stream is closed")
            self._writer.write(payload)
            try:
                if self._timeout is None:
                    await self._writer.drain()
                else:
                    await asyncio.wait_for(
                        self._writer.drain(),
                        timeout=self._timeout,
                    )
            except asyncio.TimeoutError as error:
                raise SipTimeoutError("Timed out sending a SIP message") from error
            except OSError as error:
                raise SipTransportError("Could not send a SIP message") from error

    async def receive(self) -> SipMessage:
        """Receive one framed SIP message."""
        return await self._reader.read_message()

    async def close(self) -> None:
        """Close the writer and wait until the TLS transport has closed."""
        async with self._write_lock:
            if not self._writer.is_closing():
                self._writer.close()
            try:
                if self._timeout is None:
                    await self._writer.wait_closed()
                else:
                    await asyncio.wait_for(
                        self._writer.wait_closed(),
                        timeout=self._timeout,
                    )
            except asyncio.TimeoutError as error:
                raise SipTimeoutError("Timed out closing the SIP TLS stream") from error
            except OSError as error:
                raise SipTransportError("Could not close the SIP TLS stream") from error
