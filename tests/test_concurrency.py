"""Unit tests for the sync-pipeline concurrency layer.

Covers the four DoD invariants:
- Cache-hit skips OpenAI (and is cheap enough to not need the semaphore)
- Cache-miss populates cache
- 429 retry path recovers without crashing the caller
- Concurrency limit is actually enforced (max N in-flight at any time)

No real OpenAI/ES calls. Mocks the async SDK surface narrowly so the
tests double as contract documentation for the async call sites.
"""
from __future__ import annotations

import asyncio
import os
import time
from typing import Any
from unittest.mock import patch

import pytest

from botnim._concurrency import (
    SyncConcurrency,
    async_retry_openai,
    get_sync_concurrency,
)


# ---------------------------------------------------------------------------
# get_sync_concurrency()
# ---------------------------------------------------------------------------

def test_get_sync_concurrency_default(monkeypatch):
    monkeypatch.delenv("SYNC_CONCURRENCY", raising=False)
    assert get_sync_concurrency() == 10


def test_get_sync_concurrency_env_override(monkeypatch):
    monkeypatch.setenv("SYNC_CONCURRENCY", "3")
    assert get_sync_concurrency() == 3


def test_get_sync_concurrency_invalid_falls_back(monkeypatch):
    monkeypatch.setenv("SYNC_CONCURRENCY", "not-a-number")
    assert get_sync_concurrency() == 10


def test_get_sync_concurrency_clamps_below_one(monkeypatch):
    monkeypatch.setenv("SYNC_CONCURRENCY", "0")
    assert get_sync_concurrency() == 1


# ---------------------------------------------------------------------------
# SyncConcurrency.run_bounded — max N in-flight at any time
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_semaphore_enforces_concurrency_limit():
    """DoD: concurrency limit enforced (max N in-flight at any time)."""
    concurrency = SyncConcurrency(concurrency=3)
    in_flight = 0
    peak = 0
    lock = asyncio.Lock()

    async def work(i: int) -> int:
        nonlocal in_flight, peak
        async with lock:
            in_flight += 1
            peak = max(peak, in_flight)
        # Yield so other tasks have a chance to run; if the semaphore
        # isn't holding them back, `in_flight` will exceed 3.
        await asyncio.sleep(0.02)
        async with lock:
            in_flight -= 1
        return i

    results = await asyncio.gather(
        *[concurrency.run_bounded(work, i) for i in range(20)]
    )
    assert results == list(range(20))  # order preserved by gather
    assert peak <= 3, f"peak in-flight = {peak}, should be ≤ 3"
    assert peak == 3, "semaphore should allow the configured max, not less"


@pytest.mark.asyncio
async def test_concurrency_one_is_fully_serial():
    """DoD criterion #3: SYNC_CONCURRENCY=1 ⇒ byte-equal to serial code.

    We can't assert byte-equality of ES state from unit tests — the
    narrower invariant we DO check here is that concurrency=1 actually
    serializes, so no race can reorder work.
    """
    concurrency = SyncConcurrency(concurrency=1)
    in_flight = 0
    peak = 0

    async def work() -> None:
        nonlocal in_flight, peak
        in_flight += 1
        peak = max(peak, in_flight)
        await asyncio.sleep(0.01)
        in_flight -= 1

    await asyncio.gather(*[concurrency.run_bounded(work) for _ in range(10)])
    assert peak == 1


# ---------------------------------------------------------------------------
# Cache-before-acquire (the concrete path lives in collect_sources +
# vector_store_es; here we verify the primitive the tests above rely on).
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_cache_hits_skip_the_pool():
    """DoD: cache-hits do not consume semaphore slots.

    Simulates the pattern used in ``_get_metadata_for_content_async`` and
    ``_build_embedding_async``: check the cache first, only call
    ``run_bounded`` on a miss.
    """
    concurrency = SyncConcurrency(concurrency=2)
    cache: dict[str, str] = {"cached-key": "cached-value"}
    openai_calls = 0

    async def expensive(key: str) -> str:
        nonlocal openai_calls
        openai_calls += 1
        await asyncio.sleep(0.01)
        return f"fresh-{key}"

    async def cached_or_fresh(key: str) -> str:
        if key in cache:
            return cache[key]
        result = await concurrency.run_bounded(expensive, key)
        cache[key] = result
        return result

    keys = ["cached-key"] * 100 + ["miss-1", "miss-2"]
    results = await asyncio.gather(*[cached_or_fresh(k) for k in keys])
    assert results[:100] == ["cached-value"] * 100
    assert results[100] == "fresh-miss-1"
    assert openai_calls == 2, f"should only call OpenAI for the 2 misses, got {openai_calls}"


# ---------------------------------------------------------------------------
# async_retry_openai — 429 retry path
# ---------------------------------------------------------------------------

class _FakeRateLimit(Exception):
    """Stand-in for openai.RateLimitError (we don't want to import the real
    one here — the decorator detects it via message substring, matching
    production behavior)."""
    def __init__(self) -> None:
        super().__init__("Error code: 429 - Rate limit reached for gpt-4o-mini")


@pytest.mark.asyncio
async def test_retry_recovers_from_429():
    """DoD: hitting OpenAI rate limits does not crash the sync."""
    attempts = 0

    @async_retry_openai(max_retries=5, initial_delay=0.001, max_delay=0.01)
    async def flaky_call() -> str:
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise _FakeRateLimit()
        return "ok"

    result = await flaky_call()
    assert result == "ok"
    assert attempts == 3, f"should have retried through two 429s + 1 success, got {attempts}"


@pytest.mark.asyncio
async def test_retry_gives_up_after_max_retries_and_raises():
    """Transient error that never resolves: propagate to the caller so the
    individual document ends up in the extraction_error branch, rather
    than silently succeeding with garbage."""
    attempts = 0

    @async_retry_openai(max_retries=3, initial_delay=0.001, max_delay=0.01)
    async def always_fails() -> str:
        nonlocal attempts
        attempts += 1
        raise _FakeRateLimit()

    with pytest.raises(_FakeRateLimit):
        await always_fails()
    assert attempts == 3


@pytest.mark.asyncio
async def test_retry_does_not_retry_non_transient():
    """Non-429, non-5xx errors should bubble up immediately so callers see
    the real problem (auth errors, bad payloads, etc.)."""
    attempts = 0

    class HardError(Exception):
        pass

    @async_retry_openai(max_retries=5, initial_delay=0.001)
    async def hard_failure() -> str:
        nonlocal attempts
        attempts += 1
        raise HardError("invalid api key")

    with pytest.raises(HardError):
        await hard_failure()
    assert attempts == 1, "non-transient errors should not be retried"


# ---------------------------------------------------------------------------
# Error isolation (gather with return_exceptions=True)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_error_isolation_one_failure_does_not_poison_batch():
    """DoD: one failed doc does NOT poison the batch.

    The extraction / upload pipelines use ``asyncio.gather(..., return_exceptions=True)``
    and filter out BaseException results. This test verifies the
    primitive behaves as expected so the code sites can rely on it.
    """
    async def maybe_fail(i: int) -> int:
        if i == 5:
            raise RuntimeError(f"doc {i} is broken")
        return i

    results = await asyncio.gather(
        *[maybe_fail(i) for i in range(10)],
        return_exceptions=True,
    )
    successes = [r for r in results if not isinstance(r, BaseException)]
    failures = [r for r in results if isinstance(r, BaseException)]
    assert successes == [0, 1, 2, 3, 4, 6, 7, 8, 9]
    assert len(failures) == 1
    assert "doc 5" in str(failures[0])
