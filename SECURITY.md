# Security

This project wraps an unofficial API and is not affiliated with PJSC Tattelecom.

## Secrets

Treat the access token, SMS code, phone number, SIP credentials, and signed stream URLs
as secrets. Do not commit them, put them in issue reports, or enable HTTP body logging in
production.

## Media TLS

TLS verification is enabled by default. Some Tattelecom media-server deployments have
previously presented an incomplete certificate chain. Passing `media_verify=False` may
work around that issue, but makes media requests vulnerable to man-in-the-middle attacks.
Prefer a custom trusted CA bundle whenever possible.

## Reporting

Please report vulnerabilities privately to the repository owner before opening a public
issue.
