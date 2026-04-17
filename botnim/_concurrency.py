"""Shared async concurrency primitives for the sync pipeline.

The sync path has two OpenAI round-trips per document (metadata extraction
via chat.completions, content embedding via embeddings.create) plus one
optional description embedding. Serializing them was the ~5.5h cold-sync
bottleneck; this module provides the scaffolding for bounded-concurrency
parallelization:

- ``SyncConcurrency`` — holds the shared semaphore and cache lock used by
  extraction and indexing. One instance per top-level async run.
- ``async_retry_openai`` — decorator for OpenAI async calls, adding
  exponential-backoff-with-jitter on 429s and other transient errors.

The byte-equal-at-concurrency-1 invariant (DoD #3) relies on:
- ``asyncio.Semaphore(1)`` making calls effectively serial under load
- Inputs processed via ``asyncio.gather``, which preserves input order in
  its output — iteration order stays the same as today's serial code
- Cache writes guarded by a single lock so the sqlite KVFile never sees
  concurrent writers (sqlite-over-NFS on EFS is especially sensitive).
"""
from __future__ import annotations

import asyncio
import functools
import os
import random
from typing import Callable, Awaitable, TypeVar, Any

from .config import get_logger


logger = get_logger(__name__)

DEFAULT_SYNC_CONCURRENCY = 10


def get_sync_concurrency() -> int:
    """Read the concurrency cap from the environment.

    ``SYNC_CONCURRENCY=1`` degenerates to serial behavior and is the setting
    the byte-equality unit test pins (DoD criterion #3).
    """
    raw = os.environ.get("SYNC_CONCURRENCY", "").strip()
    if not raw:
        return DEFAULT_SYNC_CONCURRENCY
    try:
        value = int(raw)
    except ValueError:
        logger.warning(
            "SYNC_CONCURRENCY=%r is not an int; falling back to %d",
            raw, DEFAULT_SYNC_CONCURRENCY,
        )
        return DEFAULT_SYNC_CONCURRENCY
    if value < 1:
        logger.warning(
            "SYNC_CONCURRENCY=%d is < 1; clamping to 1", value,
        )
        return 1
    return value


class SyncConcurrency:
    """Shared per-run gating primitives.

    One instance is created at the start of an async sync run and threaded
    through the extraction / upload code paths. Cheap to create; don't
    cache across runs.
    """

    def __init__(self, concurrency: int | None = None) -> None:
        self.concurrency = concurrency if concurrency is not None else get_sync_concurrency()
        # Bounds in-flight OpenAI calls (extraction + embeddings share this).
        # One pool keeps the overall rate predictable; splitting by API
        # would be a second-order optimization.
        self.semaphore = asyncio.Semaphore(self.concurrency)
        # Guards writes to the sqlite KVFile caches. Reads are lock-free
        # because sqlite handles concurrent readers fine; only the write
        # path needs serialization to avoid "database is locked" errors.
        self.cache_lock = asyncio.Lock()

    async def run_bounded(
        self,
        fn: Callable[..., Awaitable[Any]],
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        """Run ``fn(*args, **kwargs)`` under the semaphore."""
        async with self.semaphore:
            return await fn(*args, **kwargs)


T = TypeVar("T")


def async_retry_openai(
    max_retries: int = 6,
    initial_delay: float = 1.0,
    max_delay: float = 60.0,
) -> Callable[[Callable[..., Awaitable[T]]], Callable[..., Awaitable[T]]]:
    """Decorate an async OpenAI call with exponential backoff on 429s.

    The caller must already be inside the concurrency semaphore when this
    runs so retries don't consume additional concurrency beyond the
    in-flight slot the caller already holds.
    """

    def decorator(fn: Callable[..., Awaitable[T]]) -> Callable[..., Awaitable[T]]:
        @functools.wraps(fn)
        async def wrapper(*args: Any, **kwargs: Any) -> T:
            delay = initial_delay
            last_exc: BaseException | None = None
            for attempt in range(max_retries):
                try:
                    return await fn(*args, **kwargs)
                except Exception as exc:
                    last_exc = exc
                    msg = str(exc).lower()
                    is_rate_limit = (
                        "429" in msg
                        or "rate limit" in msg
                        or "ratelimiterror" in type(exc).__name__.lower()
                    )
                    is_transient = (
                        is_rate_limit
                        or "timeout" in msg
                        or "connection" in msg
                        or "502" in msg
                        or "503" in msg
                        or "504" in msg
                    )
                    if not is_transient or attempt == max_retries - 1:
                        raise
                    sleep_for = min(delay + random.uniform(0, delay), max_delay)
                    logger.warning(
                        "%s: transient error (attempt %d/%d), sleeping %.2fs — %s",
                        fn.__name__, attempt + 1, max_retries, sleep_for,
                        str(exc)[:200],
                    )
                    await asyncio.sleep(sleep_for)
                    delay = min(delay * 2, max_delay)
            # Unreachable — the final attempt re-raises — but keep mypy happy.
            raise last_exc if last_exc is not None else RuntimeError("retry loop drained without exception")

        return wrapper

    return decorator


def run_async(coro: Awaitable[T]) -> T:
    """Run an async coroutine from sync code.

    Uses ``asyncio.run`` when there is no running loop (the common case
    for CLI entry points); otherwise delegates to ``asyncio.get_event_loop``
    to accommodate environments that already have a loop (e.g., notebooks).
    """
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)
    # If we're already inside a loop, create a task and block on it. This
    # path is rarely hit by the sync CLI but matters for tests that await
    # the sync wrapper directly.
    return loop.run_until_complete(coro)  # type: ignore[no-any-return]
