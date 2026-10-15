
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from collections import defaultdict, deque
from threading import Lock
from time import monotonic
from math import ceil
from uuid import uuid4


EMAIL = "22f3001026@ds.study.iitm.ac.in"

RATE_LIMIT = 12
WINDOW_SECONDS = 10

ALLOWED_ORIGINS = [
    "https://app-zpt35k.example.com",
    "https://dash-58k0n2.example.com",
]

api = FastAPI()

# Each client has an independent sliding-window bucket.
client_requests = defaultdict(deque)
rate_lock = Lock()


@api.get("/ping")
async def ping(request: Request):
    return {
        "email": EMAIL,
        "request_id": request.state.request_id,
    }


class RequestContextMiddleware:
    """Propagate an existing request ID or generate a new UUID."""

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        headers = dict(scope.get("headers", []))
        incoming_id = headers.get(b"x-request-id", b"").decode(
            "latin-1"
        )

        request_id = incoming_id if incoming_id else str(uuid4())

        # Make the ID available to FastAPI's Request object.
        scope.setdefault("state", {})["request_id"] = request_id

        async def send_with_request_id(message):
            if message["type"] == "http.response.start":
                response_headers = list(message.get("headers", []))
                response_headers = [
                    (key, value)
                    for key, value in response_headers
                    if key.lower() != b"x-request-id"
                ]
                response_headers.append(
                    (b"x-request-id", request_id.encode("latin-1"))
                )
                message["headers"] = response_headers

            await send(message)

        await self.app(scope, receive, send_with_request_id)


class RateLimitMiddleware:
    """Allow 12 requests per client in each rolling 10-second window."""

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        # Do not count browser CORS preflight requests.
        if scope["method"] == "OPTIONS":
            await self.app(scope, receive, send)
            return

        headers = dict(scope.get("headers", []))
        client_id = headers.get(
            b"x-client-id", b"anonymous"
        ).decode("latin-1")

        now = monotonic()

        with rate_lock:
            timestamps = client_requests[client_id]

            while (
                timestamps
                and now - timestamps[0] >= WINDOW_SECONDS
            ):
                timestamps.popleft()

            if len(timestamps) >= RATE_LIMIT:
                retry_after = max(
                    1,
                    ceil(
                        WINDOW_SECONDS
                        - (now - timestamps[0])
                    ),
                )

                response = (
                    b'{"detail":"Rate limit exceeded"}'
                )

                await send({
                    "type": "http.response.start",
                    "status": 429,
                    "headers": [
                        (b"content-type", b"application/json"),
                        (
                            b"retry-after",
                            str(retry_after).encode("ascii"),
                        ),
                    ],
                })

                await send({
                    "type": "http.response.body",
                    "body": response,
                })
                return

            timestamps.append(now)

        await self.app(scope, receive, send)


# Explicit middleware composition:
# Request context -> CORS -> rate limiter -> FastAPI.
# CORS wraps rate-limit responses, and request IDs wrap everything.
app = RequestContextMiddleware(
    CORSMiddleware(
        RateLimitMiddleware(api),
        allow_origins=ALLOWED_ORIGINS,
        allow_credentials=False,
        allow_methods=["GET", "OPTIONS"],
        allow_headers=[
            "X-Request-ID",
            "X-Client-Id",
            "Content-Type",
        ],
        expose_headers=["X-Request-ID", "Retry-After"],
    )
)
