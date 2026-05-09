"""End-to-end orchestrator for one sanity run.

Pure orchestration; no HTTP, no threading. Caller (the FastAPI route
handler) wraps run_sanity() in a daemon thread.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional

from botnim.sanity.alerts import evaluate_alerts
from botnim.sanity.capture import capture_pair
from botnim.sanity.gold_set import load_gold_set
from botnim.sanity.judge import judge_all
from botnim.sanity.render import render_html
from botnim.sanity.storage import (
    create_run,
    fail_run,
    finalize_run,
    list_history_for_alerts,
)
from botnim.sanity.types import (
    AlertEvaluation,
    AlertReason,
    CaptureResult,
    RunSummary,
)

logger = logging.getLogger(__name__)

# Sanity creds. The shared user is pre-baked into both Mongos.
_SANITY_USER = "user178@bonim.il"
_SANITY_PASSWORD = "rebuilding279602"


def _urls_for_env(env: str) -> tuple[str, str]:
    if env == "prod":
        return ("https://botnim.co.il", "https://botnim.build-up.team")
    if env == "staging":
        return ("https://botnim.co.il", "https://botnim.staging.build-up.team")
    raise ValueError(f"unknown env: {env}")


def run_sanity(*, env: str, db_url: str) -> str:
    url_old, url_new = _urls_for_env(env)
    run_id = create_run(db_url, env=env, url_old=url_old, url_new=url_new)
    logger.info("SANITY_RUN_CREATED: env=%s run_id=%s", env, run_id)

    try:
        gold = load_gold_set()
        capture: CaptureResult = capture_pair(
            url_old=url_old, url_new=url_new,
            user=_SANITY_USER, password=_SANITY_PASSWORD,
            gold_set=gold,
        )
        judged = judge_all(capture.rows)

        capture_dicts = [_row_to_capture_dict(r) for r in capture.rows]
        title = f"Sanity DoD — {env} — {datetime.now(tz=timezone.utc):%Y-%m-%d %H:%M UTC}"
        html = render_html(capture_dicts, judged, title=title)

        summary = _summarize(capture, judged)
        history = list_history_for_alerts(db_url, env=env, days=7)
        alerts = evaluate_alerts(summary, history)

        finalize_run(
            db_url, run_id,
            summary=summary,
            capture_json={"rows": capture_dicts},
            judged_json=judged,
            html=html,
            alerts=alerts,
        )

        if alerts.severity == "red":
            reasons_str = "; ".join(f"{r.rule}: {r.detail}" for r in alerts.reasons)
            logger.error("SANITY_REGRESSION: %s", reasons_str)

        return run_id
    except Exception as e:
        fail_run(db_url, run_id, error=f"{type(e).__name__}: {e}")
        raise


def _row_to_capture_dict(row) -> dict:
    return {
        "row": row.row,
        "question": row.question,
        "expected_behavior": row.expected_behavior,
        "must_not_contain": row.must_not_contain,
        "observed_notes": row.observed_notes,
        "answer_old": {
            "text": row.answer_old.text,
            "ok": row.answer_old.ok,
            "duration_ms": row.answer_old.duration_ms,
            "error": row.answer_old.error,
        },
        "answer_new": {
            "text": row.answer_new.text,
            "ok": row.answer_new.ok,
            "duration_ms": row.answer_new.duration_ms,
            "error": row.answer_new.error,
        },
    }


def _summarize(capture: CaptureResult, judged: dict[str, dict]) -> RunSummary:
    total = len(capture.rows)
    ab_new = ab_old = ab_tie = 0
    rb_pass = rb_fail = rb_xfail = rb_infra = 0
    for row in capture.rows:
        verdict = judged.get(str(row.row), {})
        ab = verdict.get("ab_verdict")
        if ab == "NEW":
            ab_new += 1
        elif ab == "OLD":
            ab_old += 1
        else:
            ab_tie += 1
        rv = verdict.get("rubric_verdict")
        if rv == "PASS":
            rb_pass += 1
        elif rv == "FAIL":
            rb_fail += 1
        elif rv == "XFAIL":
            rb_xfail += 1
        elif rv == "INFRA":
            rb_infra += 1
    pass_rate = (rb_pass / (rb_pass + rb_fail)) if (rb_pass + rb_fail) > 0 else None
    return RunSummary(
        total_rows=total,
        ab_new_wins=ab_new, ab_old_wins=ab_old, ab_ties=ab_tie,
        rubric_pass=rb_pass, rubric_fail=rb_fail,
        rubric_xfail=rb_xfail, rubric_infra=rb_infra,
        pass_rate=pass_rate,
    )
