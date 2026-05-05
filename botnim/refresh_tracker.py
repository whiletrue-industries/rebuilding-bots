"""In-memory refresh-progress tracker.

The /admin/refresh endpoint launches `_run_refresh_job_background` on a
daemon thread and returns 202 immediately. Operators have no in-band
visibility into what's happening — they have to tail CloudWatch and
correlate against the spec's context order.

This module exposes a single process-wide ``RefreshTracker`` singleton
that the refresh thread updates and the GET /admin/refresh/status
endpoint reads. It's intentionally an in-memory singleton (not Redis or
Aurora) because:

  - There's only ever one botnim-api task per env (single-writer to EFS
    for the extraction CSVs); a process-local tracker is sufficient.
  - State is ephemeral; the daily Lambda re-fires anyway.
  - No new infra to provision.

If we ever scale botnim-api to >1 replica, the tracker becomes
per-replica and the UI shows whichever one the load balancer routed to.
That's acceptable for an admin-only diagnostic; we'd promote it to
Redis at that point.
"""
from __future__ import annotations

import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class ContextResult:
    """Outcome of processing a single (bot, context) pair."""

    bot: str
    context: str
    status: str  # "ok" | "failed"
    started_at: str
    finished_at: str
    error_type: Optional[str] = None
    error_message: Optional[str] = None


@dataclass
class _State:
    status: str = "idle"  # "idle" | "running" | "done" | "failed"
    started_at: Optional[str] = None
    finished_at: Optional[str] = None
    current_bot: Optional[str] = None
    current_context: Optional[str] = None
    current_started_at: Optional[str] = None
    total_contexts: int = 0
    completed: list[ContextResult] = field(default_factory=list)
    last_error: Optional[str] = None  # set when status == "failed"


class RefreshTracker:
    """Thread-safe progress tracker.

    The refresh thread calls ``begin_run``, then ``begin_context`` /
    ``end_context`` per context, then ``finish_run``. Reads are
    consistent because ``snapshot()`` copies the state under the lock.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._state = _State()

    # --- writes (called from the refresh thread) ---

    def begin_run(self, total_contexts: int) -> None:
        with self._lock:
            # Don't reset ``completed`` immediately — keep the previous run's
            # results visible until we have new ones to overwrite.
            self._state = _State(
                status="running",
                started_at=_now(),
                total_contexts=total_contexts,
            )

    def begin_context(self, bot: str, context: str) -> None:
        with self._lock:
            self._state.current_bot = bot
            self._state.current_context = context
            self._state.current_started_at = _now()

    def end_context(
        self,
        bot: str,
        context: str,
        *,
        ok: bool,
        error: Optional[BaseException] = None,
    ) -> None:
        with self._lock:
            started = self._state.current_started_at or _now()
            self._state.completed.append(ContextResult(
                bot=bot,
                context=context,
                status="ok" if ok else "failed",
                started_at=started,
                finished_at=_now(),
                error_type=type(error).__name__ if error else None,
                error_message=str(error) if error else None,
            ))
            self._state.current_bot = None
            self._state.current_context = None
            self._state.current_started_at = None

    def finish_run(self, *, ok: bool, error: Optional[BaseException] = None) -> None:
        with self._lock:
            self._state.status = "done" if ok else "failed"
            self._state.finished_at = _now()
            if error is not None:
                self._state.last_error = f"{type(error).__name__}: {error}"

    # --- reads (called from the HTTP layer) ---

    def snapshot(self) -> dict:
        with self._lock:
            return {
                "status": self._state.status,
                "started_at": self._state.started_at,
                "finished_at": self._state.finished_at,
                "current": (
                    {
                        "bot": self._state.current_bot,
                        "context": self._state.current_context,
                        "started_at": self._state.current_started_at,
                    }
                    if self._state.current_context is not None
                    else None
                ),
                "total_contexts": self._state.total_contexts,
                "completed_count": len(self._state.completed),
                "ok_count": sum(1 for c in self._state.completed if c.status == "ok"),
                "failed_count": sum(1 for c in self._state.completed if c.status == "failed"),
                "completed": [
                    {
                        "bot": c.bot,
                        "context": c.context,
                        "status": c.status,
                        "started_at": c.started_at,
                        "finished_at": c.finished_at,
                        "error_type": c.error_type,
                        "error_message": c.error_message,
                    }
                    for c in self._state.completed
                ],
                "last_error": self._state.last_error,
            }


# Module-level singleton — import this from server.py and fetch_and_process.py.
TRACKER = RefreshTracker()
