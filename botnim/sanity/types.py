"""Dataclasses shared across the sanity package.

All fields use built-in types so the same shapes serialise cleanly into
JSONB columns and back without a custom encoder.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal, Optional


@dataclass
class GoldEntry:
    row: int
    question: str
    expected_behavior: str
    must_not_contain: list[str]
    observed_notes: str = ""


@dataclass
class Answer:
    text: str
    ok: bool
    duration_ms: Optional[int] = None
    error: Optional[str] = None


@dataclass
class CaptureRow:
    row: int
    question: str
    expected_behavior: str
    must_not_contain: list[str]
    observed_notes: str
    answer_old: Answer
    answer_new: Answer


@dataclass
class CaptureResult:
    rows: list[CaptureRow]


ABVerdictLabel = Literal["NEW", "OLD", "TIE"]
RubricVerdictLabel = Literal["PASS", "FAIL", "XFAIL", "INFRA"]
AlertSeverity = Optional[Literal["orange", "red"]]


@dataclass
class ABVerdict:
    verdict: ABVerdictLabel
    reason: str


@dataclass
class RubricVerdict:
    score: Optional[float]   # None when verdict == "INFRA"
    verdict: RubricVerdictLabel
    reason: str


@dataclass
class JudgedRow:
    row: int
    ab_verdict: ABVerdictLabel
    ab_reason: str
    rubric_score: Optional[float]
    rubric_verdict: RubricVerdictLabel
    rubric_reason: str


@dataclass
class RunSummary:
    total_rows: int
    ab_new_wins: int
    ab_old_wins: int
    ab_ties: int
    rubric_pass: int
    rubric_fail: int
    rubric_xfail: int
    rubric_infra: int
    pass_rate: Optional[float]   # None when (rubric_pass + rubric_fail) == 0


@dataclass
class AlertReason:
    rule: str
    detail: str


@dataclass
class AlertEvaluation:
    severity: AlertSeverity
    reasons: list[AlertReason] = field(default_factory=list)
