"""Wire-level constants for the Domofon Letai API."""

API_URL = "https://domofon.tattelecom.ru/{version}/{path}"

DEFAULT_HEADERS = {
    "Accept": "application/json",
    "Accept-Charset": "UTF-8",
    "Accept-Encoding": "gzip",
    "User-Agent": "ktor-client",
}

MEDIA_HEADERS = {
    "Accept-Encoding": "identity",
    "User-Agent": DEFAULT_HEADERS["User-Agent"],
}

DEFAULT_DEVICE_CODE = "Android_empty_push_token"
DEFAULT_DEVICE_OS_ID = 1
DEFAULT_TIMEOUT = 10.0
