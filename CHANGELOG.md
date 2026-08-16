# Changelog

## 0.3.0

- Experimental push-correlated SIP-over-TLS call control.
- Digest REGISTER and bounded zero-expiry deregistration.
- `decline()`, signaling-only `answer_inactive()`, `hangup()`, and
  `open_door_and_end()` operations.
- Strict endpoint, panel login, Call-ID, dialog, and transaction correlation.
- CANCEL, ACK, BYE, Record-Route, duplicate transaction, and Timer H/L handling.
- Dedicated call-control guide and runnable example.

## 0.2.0

- Incoming-call notifications as a typed async iterator.
- Atomic, versioned FCM credential file storage.
- Automatic FCM token registration and credential persistence during startup.
- In-memory duplicate delivery protection and bounded event queue.
- Typed SIP account metadata for external SIP clients.
- Detailed API and incoming-call documentation with a runnable example.

## 0.1.0

- SMS authentication and reusable access tokens.
- Typed intercom and building models.
- Door opening command.
- Fresh HLS and MPEG-TS stream source discovery.
- Non-buffering media streaming with MPEG-TS as the low-latency default.
- Structured API, transport, protocol, and stream errors.
