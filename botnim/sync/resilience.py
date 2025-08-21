"""
Resilience utilities: retry with exponential backoff and simple circuit breaker.

This module provides small, dependency-free helpers that can be integrated
incrementally across the sync system without invasive refactors.
"""

from __future__ import annotations

import random
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable, Dict, Optional, Tuple, Type


@dataclass
class RetryPolicy:
    max_attempts: int = 3
    base_delay_seconds: float = 1.0
    max_delay_seconds: float = 32.0
    jitter: bool = True
    retry_on_exceptions: Tuple[Type[BaseException], ...] = (Exception,)

    def compute_backoff(self, attempt_index_zero_based: int) -> float:
        delay = min(self.base_delay_seconds * (2 ** attempt_index_zero_based), self.max_delay_seconds)
        if self.jitter:
            delay = delay * (0.5 + random.random())  # 0.5x - 1.5x jitter window
        return delay


@dataclass
class CircuitState:
    failures: int = 0
    open_until: Optional[float] = None  # epoch seconds when half-open allowed


@dataclass
class CircuitBreaker:
    failure_threshold: int = 3
    reset_timeout_seconds: float = 60.0
    _state: Dict[str, CircuitState] = field(default_factory=dict)

    def is_open(self, key: str) -> bool:
        state = self._state.get(key)
        if not state:
            return False
        # If open_until is set and passed, transition to half-open (allow one attempt)
        if state.open_until is not None and time.time() >= state.open_until:
            # half-open: allow attempt; clear open_until so we can record success/failure
            state.open_until = None
            return False
        return state.failures >= self.failure_threshold and state.open_until is not None

    def record_success(self, key: str) -> None:
        if key in self._state:
            self._state[key] = CircuitState()  # reset

    def record_failure(self, key: str) -> None:
        state = self._state.setdefault(key, CircuitState())
        state.failures += 1
        if state.failures >= self.failure_threshold:
            state.open_until = time.time() + self.reset_timeout_seconds

    def get_state_snapshot(self) -> Dict[str, Dict[str, Optional[float]]]:
        snapshot: Dict[str, Dict[str, Optional[float]]] = {}
        for key, state in self._state.items():
            snapshot[key] = {
                "failures": state.failures,
                "open_until": datetime.fromtimestamp(state.open_until, tz=timezone.utc).isoformat() if state.open_until else None,
            }
        return snapshot


def with_retry(fn: Callable[[], object], *, policy: RetryPolicy, circuit_breaker: Optional[CircuitBreaker] = None, circuit_key: Optional[str] = None) -> object:
    """
    Execute function with retry and optional circuit breaker semantics.

    - If circuit breaker is provided and circuit is open for key, raises RuntimeError immediately.
    - Retries up to max_attempts on configured exceptions with exponential backoff and jitter.
    """
    if circuit_breaker and circuit_key and circuit_breaker.is_open(circuit_key):
        raise RuntimeError(f"Circuit open for {circuit_key}")

    last_exc: Optional[BaseException] = None
    for attempt in range(policy.max_attempts):
        try:
            result = fn()
            if circuit_breaker and circuit_key:
                circuit_breaker.record_success(circuit_key)
            return result
        except policy.retry_on_exceptions as exc:  # type: ignore
            last_exc = exc
            if attempt >= policy.max_attempts - 1:
                if circuit_breaker and circuit_key:
                    circuit_breaker.record_failure(circuit_key)
                break
            delay = policy.compute_backoff(attempt)
            time.sleep(delay)
        except BaseException:
            # Non-retryable exception: record failure and raise
            if circuit_breaker and circuit_key:
                circuit_breaker.record_failure(circuit_key)
            raise

    # Exhausted retries
    if last_exc:
        raise last_exc
    raise RuntimeError("with_retry exhausted without exception context")

