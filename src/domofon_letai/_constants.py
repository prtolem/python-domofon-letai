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

PUSH_SERVICE_FCM = "fcm"
PUSH_CATEGORY_START_CALL = "start_call"

FIREBASE_PROJECT_ID = "israeldidnothingwrong-80a00"
FIREBASE_APP_ID = "1:49573252933:android:d686dc8f406db347a4382c"
FIREBASE_API_KEY = "AIzaSyDvjcS4tcZo2Y3u4bdwcPTxAMn9USEHE1E"
FIREBASE_SENDER_ID = "49573252933"
