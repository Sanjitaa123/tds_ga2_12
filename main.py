
import os
import time
import uuid
import asyncio
from collections import defaultdict, deque
from urllib.parse import urlparse

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from starlette.middleware.cors import CORSMiddleware


EMAIL = "22f3001026@ds.study.iitm.ac.in"

ALLOWED_ORIGINS = [
    "https://app-zpt35k.example.com",
    "https://dash-58k0n2.example.com",
]

RATE_LIMIT = 12
WINDOW_SECONDS = 10

app = FastAPI()

# Per-client sliding-window rate limiter
request_times = defaultdict(deque)
rate_limit_lock = asyncio.Lock()


@app.middleware("http")
async def request_context(request: Request, call_next):
    request_id = request.headers.get("X-Request-ID") or str(uuid.uuid4())

    request.state.request_id = request_id

    response = await call_next(request)
    response.headers["X-Request-ID"] = request_id

    return response


@app.middleware("http")
async def rate_limiter(request: Request, call_next):
    # Let CORS preflight requests pass without consuming quota.
    if request.method == "OPTIONS":
        return await call_next(request)

    client_id = request.headers.get("X-Client-Id", "anonymous")
    now = time.monotonic()

    async with rate_limit_lock:
        timestamps = request_times[client_id]

        # Remove requests outside the 10-second window.
        while timestamps and now - timestamps[0] >= WINDOW_SECONDS:
            timestamps.popleft()

        if len(timestamps) >= RATE_LIMIT:
            retry_after = max(
                1,
                int(WINDOW_SECONDS - (now - timestamps[0]) + 0.999),
            )

            return JSONResponse(
                status_code=429,
                content={"detail": "Rate limit exceeded"},
                headers={"Retry-After": str(retry_after)},
            )

        timestamps.append(now)

    return await call_next(request)


# CORS is added last so it wraps the other middleware,
# including rate-limit error responses.
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=False,
    allow_methods=["GET", "OPTIONS"],
    allow_headers=["X-Request-ID", "X-Client-Id", "Content-Type"],
    expose_headers=["X-Request-ID"],
)


@app.get("/ping")
async def ping(request: Request):
    return {
        "email": EMAIL,
        "request_id": request.state.request_id,
    }


@app.get("/")
async def root():
    return {"status": "ok", "endpoint": "/ping"}
