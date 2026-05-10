"""Dataclasses shared across the sanity package.

All fields use built-in types so the same shapes serialise cleanly into
JSONB columns and back without a custom encoder.

Two-turn capture (post-2026-05-10):
- Each side (OLD, NEW) records up to TWO turns. Turn 1 is the user's actual
  question; turn 2 is an expansion follow-up sent on the SAME conversation,
  driven by `GoldEntry.followup_prompt`. If `followup_prompt` is None, only
  turn 1 is captured.
- Rubric verdict is one of PASS_T1 (full answer in one turn — best),
  PASS_T2 (turn 1 fell short, turn 2 satisfied `expected_after_followup`),
  FAIL, XFAIL (documented corpus gap), INFRA (capture failed).
- The legacy single-turn shape (Answer.text/ok at top level on CaptureRow)
  is preserved as fallback fields on `SideCapture` for back-compat with
  older sanity_runs JSONB rows.
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
    followup_prompt: Optional[str] = None
    expected_after_followup: Optional[str] = None


@dataclass
class Answer:
    text: str
    ok: bool
    duration_ms: Optional[int] = None
    error: Optional[str] = None


@dataclass
class SideCapture:
    """One side's (OLD or NEW) capture for a single gold-set row.

    Always has turn1; turn2 is None when the entry has no `followup_prompt`.
    """
    turn1: Answer
    turn2: Optional[Answer] = None


@dataclass
class CaptureRow:
    row: int
    question: str
    expected_behavior: str
    must_not_contain: list[str]
    observed_notes: str
    followup_prompt: Optional[str]
    expected_after_followup: Optional[str]
    answer_old: SideCapture
    answer_new: SideCapture


@dataclass
class CaptureResult:
    rows: list[CaptureRow]


ABVerdictLabel = Literal["NEW", "OLD", "TIE"]
RubricVerdictLabel = Literal["PASS_T1", "PASS_T2", "FAIL", "XFAIL", "INFRA"]
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
    rubric_pass_t1: int   # full answer in one turn
    rubric_pass_t2: int   # answer required a follow-up
    rubric_fail: int
    rubric_xfail: int
    rubric_infra: int
    pass_rate: Optional[float]   # (pass_t1 + pass_t2) / (pass_t1 + pass_t2 + fail); None if denominator is 0


@dataclass
class AlertReason:
    rule: str
    detail: str


@dataclass
class AlertEvaluation:
    severity: AlertSeverity
    reasons: list[AlertReason] = field(default_factory=list)
