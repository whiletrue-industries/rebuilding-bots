"""Aurora I/O for sanity_runs.

Uses psycopg (v3) directly (matching the project style — small surface,
no SQLAlchemy ORM overhead). All queries are single-row or small lists; no
streaming.
"""
from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional

import psycopg
import psycopg.rows

from botnim.sanity.types import (
    AlertEvaluation,
    AlertReason,
    RunSummary,
)


@dataclass
class StoredRun:
    id: str
    env: str
    started_at: datetime
    finished_at: Optional[datetime]
    status: str
    url_old: str
    url_new: str
    total_rows: Optional[int]
    ab_new_wins: Optional[int]
    ab_old_wins: Optional[int]
    ab_ties: Optional[int]
    rubric_pass: Optional[int]
    rubric_fail: Optional[int]
    rubric_xfail: Optional[int]
    rubric_infra: Optional[int]
    pass_rate: Optional[float]
    alert_severity: Optional[str]
    alert_reasons: Optional[list]
    capture_json: Optional[dict]
    judged_json: Optional[dict]
    html: Optional[str]
    error: Optional[str]


def _connect(db_url: str) -> psycopg.Connection:
    return psycopg.connect(db_url)


def create_run(
    db_url: str,
    *,
    env: str,
    url_old: str,
    url_new: str,
) -> str:
    with _connect(db_url) as conn:
        row = conn.execute(
            """
            INSERT INTO sanity_runs (env, status, url_old, url_new)
            VALUES (%s, 'running', %s, %s)
            RETURNING id
            """,
            (env, url_old, url_new),
        ).fetchone()
        return str(row[0])


def finalize_run(
    db_url: str,
    run_id: str,
    *,
    summary: RunSummary,
    capture_json: dict,
    judged_json: dict,
    html: str,
    alerts: AlertEvaluation,
) -> None:
    reasons_json = [
        {"rule": r.rule, "detail": r.detail} for r in alerts.reasons
    ]
    with _connect(db_url) as conn:
        conn.execute(
            """
            UPDATE sanity_runs SET
                status='succeeded',
                finished_at=now(),
                total_rows=%s, ab_new_wins=%s, ab_old_wins=%s, ab_ties=%s,
                rubric_pass=%s, rubric_fail=%s, rubric_xfail=%s, rubric_infra=%s,
                pass_rate=%s,
                alert_severity=%s,
                alert_reasons=%s::jsonb,
                capture_json=%s::jsonb,
                judged_json=%s::jsonb,
                html=%s
            WHERE id=%s::uuid
            """,
            (
                summary.total_rows, summary.ab_new_wins, summary.ab_old_wins,
                summary.ab_ties, summary.rubric_pass, summary.rubric_fail,
                summary.rubric_xfail, summary.rubric_infra,
                summary.pass_rate,
                alerts.severity,
                json.dumps(reasons_json),
                json.dumps(capture_json),
                json.dumps(judged_json),
                html,
                run_id,
            ),
        )


def fail_run(db_url: str, run_id: str, *, error: str) -> None:
    with _connect(db_url) as conn:
        conn.execute(
            """
            UPDATE sanity_runs SET status='failed', finished_at=now(), error=%s
            WHERE id=%s::uuid
            """,
            (error, run_id),
        )


def get_run(db_url: str, run_id: str) -> StoredRun:
    with _connect(db_url) as conn:
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                "SELECT * FROM sanity_runs WHERE id=%s::uuid",
                (run_id,),
            )
            row = cur.fetchone()
        if row is None:
            raise LookupError(f"sanity_runs row not found: {run_id}")
        return _row_to_stored(row)


def list_recent(db_url: str, *, env: str, limit: int = 100) -> list[StoredRun]:
    with _connect(db_url) as conn:
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                """
                SELECT * FROM sanity_runs
                WHERE env=%s
                ORDER BY started_at DESC
                LIMIT %s
                """,
                (env, limit),
            )
            rows = cur.fetchall()
        return [_row_to_stored(r) for r in rows]


def get_run_html(db_url: str, run_id: str) -> tuple[str, datetime]:
    with _connect(db_url) as conn:
        row = conn.execute(
            "SELECT html, started_at FROM sanity_runs WHERE id=%s::uuid",
            (run_id,),
        ).fetchone()
        if row is None or row[0] is None:
            raise LookupError(f"sanity_runs html not found: {run_id}")
        return row[0], row[1]


def list_history_for_alerts(
    db_url: str, *, env: str, days: int = 7
) -> list[RunSummary]:
    """Returns RunSummary objects for succeeded runs within the window.

    Used by runner.run_sanity to feed alerts.evaluate_alerts. INFRA-only
    runs (no PASS/FAIL rows) keep pass_rate=None and are passed through;
    evaluate_alerts filters them.
    """
    cutoff = datetime.now(tz=timezone.utc) - timedelta(days=days)
    with _connect(db_url) as conn:
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                """
                SELECT total_rows, ab_new_wins, ab_old_wins, ab_ties,
                       rubric_pass, rubric_fail, rubric_xfail, rubric_infra,
                       pass_rate
                FROM sanity_runs
                WHERE env=%s AND status='succeeded' AND started_at >= %s
                ORDER BY started_at DESC
                """,
                (env, cutoff),
            )
            rows = cur.fetchall()
        return [
            RunSummary(
                total_rows=r["total_rows"],
                ab_new_wins=r["ab_new_wins"],
                ab_old_wins=r["ab_old_wins"],
                ab_ties=r["ab_ties"],
                rubric_pass=r["rubric_pass"],
                rubric_fail=r["rubric_fail"],
                rubric_xfail=r["rubric_xfail"],
                rubric_infra=r["rubric_infra"],
                pass_rate=float(r["pass_rate"]) if r["pass_rate"] is not None else None,
            )
            for r in rows
        ]


def _row_to_stored(row) -> StoredRun:
    return StoredRun(
        id=str(row["id"]),
        env=row["env"],
        started_at=row["started_at"],
        finished_at=row["finished_at"],
        status=row["status"],
        url_old=row["url_old"],
        url_new=row["url_new"],
        total_rows=row["total_rows"],
        ab_new_wins=row["ab_new_wins"],
        ab_old_wins=row["ab_old_wins"],
        ab_ties=row["ab_ties"],
        rubric_pass=row["rubric_pass"],
        rubric_fail=row["rubric_fail"],
        rubric_xfail=row["rubric_xfail"],
        rubric_infra=row["rubric_infra"],
        pass_rate=float(row["pass_rate"]) if row["pass_rate"] is not None else None,
        alert_severity=row["alert_severity"],
        alert_reasons=row["alert_reasons"],
        capture_json=row["capture_json"],
        judged_json=row["judged_json"],
        html=row["html"],
        error=row["error"],
    )
