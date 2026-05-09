"""Alert evaluation rule for sanity_runs.

Pure function: takes a run summary + a window of prior summaries, returns
the severity and human-readable reasons. Persisted into sanity_runs.alert_*
once at finalize time and never recomputed.
"""
from __future__ import annotations

import statistics

from botnim.sanity.types import (
    AlertEvaluation,
    AlertReason,
    AlertSeverity,
    RunSummary,
)

_PASS_RATE_CLIFF_PP = 0.10
_MIN_PRIOR_RUNS_FOR_CLIFF = 3


def evaluate_alerts(
    this_run: RunSummary,
    history_7d: list[RunSummary],
) -> AlertEvaluation:
    reasons: list[AlertReason] = []
    severity: AlertSeverity = None

    # R1 — pass-rate cliff vs rolling 7-day median
    prior_pass_rates = [
        r.pass_rate for r in history_7d if r.pass_rate is not None
    ]
    if (
        this_run.pass_rate is not None
        and len(prior_pass_rates) >= _MIN_PRIOR_RUNS_FOR_CLIFF
    ):
        median_prior = statistics.median(prior_pass_rates)
        drop = median_prior - this_run.pass_rate
        if drop >= _PASS_RATE_CLIFF_PP:
            reasons.append(
                AlertReason(
                    rule="pass_rate_cliff",
                    detail=(
                        f"pass rate {this_run.pass_rate:.0%} is "
                        f"{drop * 100:.0f}pp below 7-day median "
                        f"{median_prior:.0%}"
                    ),
                )
            )
            severity = "red"

    # R2 — OLD wins more rows than NEW
    if this_run.ab_old_wins > this_run.ab_new_wins:
        reasons.append(
            AlertReason(
                rule="old_wins_majority",
                detail=(
                    f"OLD won {this_run.ab_old_wins}, "
                    f"NEW won {this_run.ab_new_wins} "
                    f"(legacy bot answered better on more rows)"
                ),
            )
        )
        severity = "red"

    # R3 — capture failures on NEW (orange only; doesn't downgrade red)
    if this_run.rubric_infra > 0:
        reasons.append(
            AlertReason(
                rule="capture_failed",
                detail=(
                    f"NEW failed to answer {this_run.rubric_infra} of "
                    f"{this_run.total_rows} questions (chat pipeline issue)"
                ),
            )
        )
        if severity is None:
            severity = "orange"

    return AlertEvaluation(severity=severity, reasons=reasons)
