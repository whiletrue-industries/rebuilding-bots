"""Runner orchestrates capture → judge → render → alert eval → finalize.

All four collaborators are monkey-patched. We assert lifecycle and error
handling, not the inner mechanics (those have their own tests).
"""
from __future__ import annotations

import logging

import pytest

from botnim.sanity import runner
from botnim.sanity.types import (
    AlertEvaluation,
    AlertReason,
    Answer,
    CaptureResult,
    CaptureRow,
    GoldEntry,
)


@pytest.fixture
def fake_pipeline(monkeypatch):
    """Replace capture/judge/render/storage with happy-path fakes."""
    captured = {}

    def fake_load_gold_set():
        return [GoldEntry(row=0, question="q", expected_behavior="e",
                          must_not_contain=[], observed_notes="")]

    def fake_capture_pair(**kwargs):
        captured["capture_kwargs"] = kwargs
        return CaptureResult(rows=[CaptureRow(
            row=0, question="q", expected_behavior="e",
            must_not_contain=[], observed_notes="",
            answer_old=Answer(text="old", ok=True),
            answer_new=Answer(text="new", ok=True),
        )])

    def fake_judge_all(rows):
        return {"0": {
            "ab_verdict": "NEW", "ab_reason": "x",
            "rubric_score": 0.9, "rubric_verdict": "PASS", "rubric_reason": "y",
        }}

    def fake_render_html(capture, judged, *, title):
        captured["render_args"] = (capture, judged, title)
        return "<!doctype html><body>hi</body>"

    storage_calls = []

    def fake_create_run(db_url, **kwargs):
        storage_calls.append(("create", kwargs))
        return "fake-run-id"

    def fake_finalize_run(db_url, run_id, **kwargs):
        storage_calls.append(("finalize", run_id, kwargs))

    def fake_fail_run(db_url, run_id, *, error):
        storage_calls.append(("fail", run_id, error))

    def fake_list_history(db_url, *, env, days):
        return []  # no priors → R1 silent

    monkeypatch.setattr(runner, "load_gold_set", fake_load_gold_set)
    monkeypatch.setattr(runner, "capture_pair", fake_capture_pair)
    monkeypatch.setattr(runner, "judge_all", fake_judge_all)
    monkeypatch.setattr(runner, "render_html", fake_render_html)
    monkeypatch.setattr(runner, "create_run", fake_create_run)
    monkeypatch.setattr(runner, "finalize_run", fake_finalize_run)
    monkeypatch.setattr(runner, "fail_run", fake_fail_run)
    monkeypatch.setattr(runner, "list_history_for_alerts", fake_list_history)

    return {"captured": captured, "storage": storage_calls}


def test_run_sanity_happy_path_finalizes_succeeded(fake_pipeline):
    run_id = runner.run_sanity(env="staging", db_url="postgres://fake")
    assert run_id == "fake-run-id"
    storage = fake_pipeline["storage"]
    assert storage[0][0] == "create"
    assert storage[1][0] == "finalize"
    finalize_kwargs = storage[1][2]
    assert finalize_kwargs["html"].startswith("<!doctype")
    assert finalize_kwargs["summary"].total_rows == 1


def test_run_sanity_uses_env_specific_urls(fake_pipeline):
    runner.run_sanity(env="prod", db_url="postgres://fake")
    capture_kwargs = fake_pipeline["captured"]["capture_kwargs"]
    assert capture_kwargs["url_old"] == "https://botnim.co.il"
    assert capture_kwargs["url_new"] == "https://botnim.build-up.team"


def test_run_sanity_staging_uses_staging_url(fake_pipeline):
    runner.run_sanity(env="staging", db_url="postgres://fake")
    capture_kwargs = fake_pipeline["captured"]["capture_kwargs"]
    assert capture_kwargs["url_new"] == "https://botnim.staging.build-up.team"


def test_run_sanity_capture_failure_marks_failed(monkeypatch, caplog):
    def boom(**kwargs):
        raise RuntimeError("playwright crashed")
    monkeypatch.setattr(runner, "load_gold_set", lambda: [GoldEntry(0, "q", "e", [], "")])
    monkeypatch.setattr(runner, "capture_pair", boom)
    monkeypatch.setattr(runner, "create_run", lambda db_url, **k: "rid")
    fail_calls = []
    monkeypatch.setattr(runner, "fail_run", lambda db_url, rid, *, error: fail_calls.append((rid, error)))

    with caplog.at_level(logging.ERROR), pytest.raises(RuntimeError, match="playwright crashed"):
        runner.run_sanity(env="staging", db_url="postgres://fake")

    assert fail_calls == [("rid", "RuntimeError: playwright crashed")]


def test_run_sanity_red_alert_emits_regression_log(monkeypatch, fake_pipeline, caplog):
    """When alert_severity=red, runner emits SANITY_REGRESSION at ERROR level."""
    monkeypatch.setattr(
        runner, "evaluate_alerts",
        lambda this_run, history: AlertEvaluation(
            severity="red",
            reasons=[AlertReason(rule="old_wins_majority", detail="OLD won 6, NEW won 4")],
        ),
    )
    with caplog.at_level(logging.ERROR):
        runner.run_sanity(env="staging", db_url="postgres://fake")
    assert any("SANITY_REGRESSION" in rec.message for rec in caplog.records)


def test_summary_excludes_xfail_from_pass_rate(fake_pipeline, monkeypatch):
    """If a row is XFAIL, it is NOT counted in pass_rate denominator."""
    monkeypatch.setattr(runner, "judge_all", lambda rows: {
        "0": {"ab_verdict": "NEW", "ab_reason": "", "rubric_score": 0.3, "rubric_verdict": "XFAIL", "rubric_reason": ""},
    })
    runner.run_sanity(env="staging", db_url="postgres://fake")
    storage = fake_pipeline["storage"]
    finalize_kwargs = storage[1][2]
    summary = finalize_kwargs["summary"]
    assert summary.rubric_xfail == 1
    assert summary.pass_rate is None  # 0 pass + 0 fail
