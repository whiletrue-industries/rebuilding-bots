"""Round-trip tests for sanity_runs storage."""
from __future__ import annotations

import json

import pytest

from botnim.sanity import storage
from botnim.sanity.types import (
    AlertEvaluation,
    AlertReason,
    JudgedRow,
    RunSummary,
)


@pytest.fixture
def db_url(database_url):
    """Adapt the project's database_url fixture (psycopg3 dialect URL) to a
    plain postgresql:// URL that psycopg3's connect() accepts directly.

    The project conftest yields URLs like:
        postgresql+psycopg://user:pass@host:port/dbname
    Strip the '+psycopg' dialect suffix so psycopg3's connect() accepts it.
    """
    return database_url.replace("postgresql+psycopg://", "postgresql://", 1)


def test_create_run_returns_uuid_and_inserts_running_row(db_url):
    run_id = storage.create_run(
        db_url,
        env="staging",
        url_old="https://botnim.co.il",
        url_new="https://botnim.staging.build-up.team",
    )
    assert run_id  # UUID returned
    row = storage.get_run(db_url, run_id)
    assert row.status == "running"
    assert row.env == "staging"
    assert row.url_old == "https://botnim.co.il"
    assert row.url_new == "https://botnim.staging.build-up.team"
    assert row.finished_at is None


def test_finalize_run_round_trips_counters_and_blobs(db_url):
    run_id = storage.create_run(
        db_url,
        env="staging",
        url_old="https://botnim.co.il",
        url_new="https://botnim.staging.build-up.team",
    )
    summary = RunSummary(
        total_rows=11, ab_new_wins=5, ab_old_wins=3, ab_ties=3,
        rubric_pass=8, rubric_fail=1, rubric_xfail=2, rubric_infra=0,
        pass_rate=0.889,
    )
    capture = {"rows": [{"row": 0, "question": "q"}]}
    judged = {"0": {"ab_verdict": "NEW", "rubric_score": 0.9}}
    alerts = AlertEvaluation(
        severity="red",
        reasons=[AlertReason(rule="pass_rate_cliff", detail="…")],
    )
    storage.finalize_run(
        db_url, run_id,
        summary=summary,
        capture_json=capture,
        judged_json=judged,
        html="<!doctype html><p>hi</p>",
        alerts=alerts,
    )
    row = storage.get_run(db_url, run_id)
    assert row.status == "succeeded"
    assert row.finished_at is not None
    assert row.total_rows == 11
    assert row.ab_new_wins == 5
    assert float(row.pass_rate) == pytest.approx(0.889, abs=1e-3)
    assert row.alert_severity == "red"
    assert row.alert_reasons[0]["rule"] == "pass_rate_cliff"
    assert row.capture_json == capture
    assert row.judged_json == judged
    assert row.html.startswith("<!doctype")


def test_fail_run_records_error(db_url):
    run_id = storage.create_run(
        db_url, env="staging",
        url_old="https://o", url_new="https://n",
    )
    storage.fail_run(db_url, run_id, error="capture exploded: foo")
    row = storage.get_run(db_url, run_id)
    assert row.status == "failed"
    assert row.error == "capture exploded: foo"


def test_list_recent_orders_desc_and_filters_by_env(db_url):
    a = storage.create_run(db_url, env="staging", url_old="o", url_new="n")
    b = storage.create_run(db_url, env="prod",    url_old="o", url_new="n")
    c = storage.create_run(db_url, env="staging", url_old="o", url_new="n")
    rows = storage.list_recent(db_url, env="staging", limit=10)
    assert [r.id for r in rows] == [c, a]
    assert all(r.env == "staging" for r in rows)


def test_get_run_html_returns_html_and_started_at(db_url):
    run_id = storage.create_run(db_url, env="staging", url_old="o", url_new="n")
    storage.finalize_run(
        db_url, run_id,
        summary=RunSummary(11, 5, 3, 3, 8, 1, 2, 0, 0.9),
        capture_json={}, judged_json={},
        html="<!doctype html><p>hello</p>",
        alerts=AlertEvaluation(severity=None, reasons=[]),
    )
    html, started_at = storage.get_run_html(db_url, run_id)
    assert html == "<!doctype html><p>hello</p>"
    assert started_at is not None


def test_get_run_html_missing_id_raises(db_url):
    with pytest.raises(LookupError):
        storage.get_run_html(db_url, "00000000-0000-0000-0000-000000000000")


def test_history_for_alerts_returns_succeeded_only(db_url):
    """list_history_for_alerts is what the runner feeds into evaluate_alerts."""
    a = storage.create_run(db_url, env="staging", url_old="o", url_new="n")
    storage.finalize_run(
        db_url, a,
        summary=RunSummary(11, 5, 3, 3, 8, 1, 2, 0, 0.9),
        capture_json={}, judged_json={},
        html="", alerts=AlertEvaluation(None, []),
    )
    b = storage.create_run(db_url, env="staging", url_old="o", url_new="n")
    storage.fail_run(db_url, b, error="boom")
    summaries = storage.list_history_for_alerts(db_url, env="staging", days=7)
    assert len(summaries) == 1
    assert summaries[0].pass_rate is not None
