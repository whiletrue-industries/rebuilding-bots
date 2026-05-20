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
DEFAULT_REWARM_BUDGET = 2000
# Per-RUN hard ceiling on TOTAL extraction LLM calls (rewarms + misses)
# across ALL contexts in one sync run. Defense-in-depth circuit breaker:
# the rewarm budget only caps the stale→current re-warm path; genuinely-new
# chunks (llm_misses) are otherwise uncapped. A content_hash-instability bug
# (see the 2026-05-20 `upstream_hash` poison fix) can route an entire corpus
# through the miss path.
#
# This ceiling is enforced via a single RunBudget object shared across every
# context (see RunBudget). A full cold sync of all contexts is ~110K chunks
# (knesset_protocols ~104K + the rest); 25000 caps a runaway at ~$10-12 and
# lets a legitimate EXTRACTION_VERSION bump warm over ~4-5 daily refreshes.
# Tripping it is an alert, not a steady state — it means "investigate".
# Operators doing an intentional full re-extraction raise it for one run.
DEFAULT_LLM_CALL_CEILING = 25000


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


def get_rewarm_budget() -> int:
    """Read the per-run cache-rewarm budget from the environment.

    Caps how many stale (older-version) cache hits this sync run will
    re-extract at the current ``EXTRACTION_VERSION``. ``0`` disables
    re-warming entirely (stale rows continue to serve indefinitely).
    See ``docs/superpowers/specs/2026-05-19-extraction-cache-delta-design.md``
    for the rationale and default sizing (~7 days to drain a 13K-row
    corpus at one refresh per env per day).
    """
    raw = os.environ.get("EXTRACTION_REWARM_MAX_PER_RUN", "").strip()
    if not raw:
        return DEFAULT_REWARM_BUDGET
    try:
        value = int(raw)
    except ValueError:
        logger.warning(
            "EXTRACTION_REWARM_MAX_PER_RUN=%r is not an int; falling back to %d",
            raw, DEFAULT_REWARM_BUDGET,
        )
        return DEFAULT_REWARM_BUDGET
    if value < 0:
        logger.warning(
            "EXTRACTION_REWARM_MAX_PER_RUN=%d is < 0; clamping to 0", value,
        )
        return 0
    return value


def get_llm_call_ceiling() -> int:
    """Read the per-RUN total-LLM-call circuit-breaker ceiling.

    Caps rewarms + llm_misses combined, across all contexts in one sync
    run. ``0`` disables the breaker (uncapped — the pre-2026-05-20
    behavior). See DEFAULT_LLM_CALL_CEILING.
    """
    raw = os.environ.get("EXTRACTION_MAX_LLM_CALLS_PER_RUN", "").strip()
    if not raw:
        return DEFAULT_LLM_CALL_CEILING
    try:
        value = int(raw)
    except ValueError:
        logger.warning(
            "EXTRACTION_MAX_LLM_CALLS_PER_RUN=%r is not an int; falling back to %d",
            raw, DEFAULT_LLM_CALL_CEILING,
        )
        return DEFAULT_LLM_CALL_CEILING
    if value < 0:
        logger.warning(
            "EXTRACTION_MAX_LLM_CALLS_PER_RUN=%d is < 0; clamping to 0", value,
        )
        return 0
    return value


class RunBudget:
    """Per-sync-run LLM-call ceiling, shared across all contexts.

    ``SyncConcurrency`` is constructed once *per context* (each context's
    ``collect_context_sources`` spins its own ``asyncio.run`` event loop),
    so a ceiling living on ``SyncConcurrency`` alone is per-context — with
    ~10 contexts the effective cap is 10× the intended value. To make the
    circuit breaker genuinely per-RUN, ``vector_store_update`` creates ONE
    ``RunBudget`` and threads it through every ``collect_context_sources``
    call; all contexts draw their ``llm_call_permit()`` slots from this
    single counter.

    Thread-safety: contexts are processed sequentially (one ``asyncio.run``
    fully completes before the next starts), and within a context the
    owning ``SyncConcurrency._llm_lock`` serializes access. So at any
    instant only one event loop touches this object, under one lock — a
    plain ``int`` is sufficient, no extra lock needed here.

    A ceiling of 0 disables the breaker (uncapped).
    """

    def __init__(self, llm_call_ceiling: int | None = None) -> None:
        self.llm_call_ceiling = (
            llm_call_ceiling if llm_call_ceiling is not None else get_llm_call_ceiling()
        )
        self.llm_calls_made = 0
        self.circuit_broken = False


class SyncConcurrency:
    """Shared per-run gating primitives.

    One instance is created at the start of an async sync run and threaded
    through the extraction / upload code paths. Cheap to create; don't
    cache across runs.
    """

    def __init__(
        self,
        concurrency: int | None = None,
        rewarm_budget: int | None = None,
        llm_call_ceiling: int | None = None,
        run_budget: "RunBudget | None" = None,
    ) -> None:
        self.concurrency = concurrency if concurrency is not None else get_sync_concurrency()
        # Bounds in-flight OpenAI calls (extraction + embeddings share this).
        # One pool keeps the overall rate predictable; splitting by API
        # would be a second-order optimization.
        self.semaphore = asyncio.Semaphore(self.concurrency)
        # Guards writes to the sqlite KVFile caches. Reads are lock-free
        # because sqlite handles concurrent readers fine; only the write
        # path needs serialization to avoid "database is locked" errors.
        self.cache_lock = asyncio.Lock()
        # Set by the first task that converts a 429 to RpdExhausted; once
        # set, sibling tasks short-circuit instead of burning their slots
        # on calls guaranteed to fail until UTC midnight.
        self.rpd_tripped: asyncio.Event = asyncio.Event()

        # Cache-rewarm budget: caps how many stale (older-EXTRACTION_VERSION)
        # cache hits this run will re-extract at the current version. See
        # rewarm_budget_take() and the 2026-05-19 extraction-cache-delta
        # design spec for the full rationale.
        self._rewarm_remaining = (
            rewarm_budget if rewarm_budget is not None else get_rewarm_budget()
        )
        self._rewarm_lock = asyncio.Lock()
        # Re-warm tasks scheduled by _get_metadata_for_content_async. The
        # orchestrator drains these before returning, otherwise the
        # asyncio.run() event loop closes mid-LLM-call and the tasks get
        # cancelled — leaving rewarmed_count=0 even though budget was taken.
        self.rewarm_tasks: list[asyncio.Task] = []
        # Circuit breaker: hard ceiling on TOTAL extraction LLM calls
        # (rewarms + misses). Defense-in-depth against a content_hash-
        # instability bug routing a whole corpus through the uncapped
        # llm_miss path. The ceiling is enforced against a RunBudget —
        # shared across all contexts when the orchestrator passes one in,
        # so the cap is genuinely per-RUN. Callers/tests that don't pass a
        # run_budget (or pass llm_call_ceiling directly) get a private
        # per-instance budget — per-context, fine for unit tests.
        if run_budget is not None:
            self._run_budget = run_budget
        else:
            self._run_budget = RunBudget(llm_call_ceiling=llm_call_ceiling)
        self._llm_lock = asyncio.Lock()

        # Observability counters — written by _get_metadata_for_content_async
        # and _rewarm_extraction; read at end-of-run for the
        # EXTRACTION_CACHE_SUMMARY log line.
        self.exact_hits_count = 0
        self.stale_served_count = 0
        self.rewarmed_count = 0
        self.llm_miss_count = 0
        self.circuit_skipped_count = 0

    async def llm_call_permit(self) -> bool:
        """Atomically claim a slot from the per-RUN total-LLM-call ceiling.

        Returns ``True`` if an extraction LLM call may proceed, ``False``
        if the circuit breaker has tripped (caller must NOT make the call;
        it serves stale if it can, else returns an error metadata record).

        The counter lives on the shared RunBudget, so the cap spans every
        context in the sync run. A ceiling of 0 disables the breaker
        entirely (uncapped). The first trip logs a loud
        EXTRACTION_CIRCUIT_BREAKER line — it means a bug, not a steady state.
        """
        rb = self._run_budget
        if rb.llm_call_ceiling <= 0:
            return True
        async with self._llm_lock:
            if rb.llm_calls_made >= rb.llm_call_ceiling:
                if not rb.circuit_broken:
                    rb.circuit_broken = True
                    logger.error(
                        "EXTRACTION_CIRCUIT_BREAKER: hit %d LLM-call ceiling for "
                        "this sync run (all contexts combined) — remaining chunks "
                        "served stale or skipped. This indicates content_hash "
                        "churn or a cold corpus far larger than expected; "
                        "investigate before the next refresh.",
                        rb.llm_call_ceiling,
                    )
                return False
            rb.llm_calls_made += 1
            return True

    @property
    def llm_calls_made(self) -> int:
        """Run-wide LLM calls consumed so far — for summary logs."""
        return self._run_budget.llm_calls_made

    @property
    def circuit_broken(self) -> bool:
        """Whether the per-run circuit breaker has tripped."""
        return self._run_budget.circuit_broken

    async def rewarm_budget_take(self) -> bool:
        """Atomically decrement the per-run re-warm budget.

        Returns ``True`` if a slot was available (caller should schedule a
        background re-extract at the current ``EXTRACTION_VERSION``);
        ``False`` otherwise (caller serves stale + skips re-warm).

        Always counts the stale-served event regardless of whether a slot
        was available, so ``stale_served_count`` reflects total stale
        reads in the run.
        """
        async with self._rewarm_lock:
            self.stale_served_count += 1
            if self._rewarm_remaining <= 0:
                return False
            self._rewarm_remaining -= 1
            return True

    @property
    def rewarm_budget_remaining(self) -> int:
        """Read-only view of remaining re-warm slots — for summary logs."""
        return self._rewarm_remaining

    async def run_bounded(
        self,
        fn: Callable[..., Awaitable[Any]],
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        """Run ``fn(*args, **kwargs)`` under the semaphore."""
        if self.rpd_tripped.is_set():
            # Spec: avoid burning remaining tasks on guaranteed-to-fail calls.
            from .dynamic_extraction import RpdExhausted
            raise RpdExhausted("rpd_tripped flag set by an earlier task")
        async with self.semaphore:
            try:
                return await fn(*args, **kwargs)
            except Exception:
                # If THIS call hit RPD, the caller (e.g.
                # _get_metadata_for_content_async in the next task) sets
                # ``rpd_tripped`` so siblings short-circuit. The exception
                # bubbles either way.
                raise


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
                    if is_rate_limit:
                        # Local import: avoid circular at module load (the
                        # dynamic_extraction module imports async_retry_openai
                        # from this module).
                        from .dynamic_extraction import _is_rpd_error, RpdExhausted
                        if _is_rpd_error(exc):
                            # Daily limit doesn't reset until midnight UTC;
                            # retrying within this run is pointless. Convert
                            # to RpdExhausted so the caller can short-circuit
                            # gracefully and persist partial progress.
                            raise RpdExhausted(str(exc)) from exc
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
