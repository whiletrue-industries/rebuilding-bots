"""Integration test for the snapshot writer in sync.py."""
import os
import subprocess
from pathlib import Path

import pytest
from sqlalchemy import text

from botnim.db.session import get_session
from botnim.sync import _write_snapshots


REPO_ROOT = Path(__file__).resolve().parent.parent


def _alembic_upgrade_head(database_url: str) -> None:
    env = os.environ.copy()
    env["DATABASE_URL"] = database_url
    subprocess.run(
        ["alembic", "--config", "alembic.ini", "upgrade", "head"],
        cwd=REPO_ROOT,
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )


def test_write_snapshots_inserts_per_source_and_aggregate(database_url, monkeypatch):
    """Given some documents, _write_snapshots should write one row per
    (context, source_id) plus one aggregate '*' row per context."""
    _alembic_upgrade_head(database_url)
    monkeypatch.setenv("DATABASE_URL", database_url)
    # Fresh engine per test — DATABASE_URL changed
    import botnim.db.session
    botnim.db.session._engine = None
    botnim.db.session._SessionFactory = None

    with get_session() as sess:
        cid_legal = sess.execute(text(
            "INSERT INTO contexts (bot, name) VALUES ('unified', 'legal_text') RETURNING id"
        )).scalar()
        cid_eth = sess.execute(text(
            "INSERT INTO contexts (bot, name) VALUES ('unified', 'ethics_decisions') RETURNING id"
        )).scalar()
        # 2 legal_text docs from חוק_הכנסת + 1 from תקנון_הכנסת + 3 ethics docs
        for src, n in [("חוק_הכנסת", 2), ("תקנון_הכנסת", 1)]:
            for i in range(n):
                sess.execute(text(
                    "INSERT INTO documents (context_id, content, content_hash, source_id) "
                    "VALUES (:c, :body, :h, :s)"
                ), {"c": cid_legal, "body": f"x{i}", "h": f"h{src}{i}", "s": src})
        for i in range(3):
            sess.execute(text(
                "INSERT INTO documents (context_id, content, content_hash, source_id) "
                "VALUES (:c, :body, :h, 'pdf_ethics')"
            ), {"c": cid_eth, "body": f"e{i}", "h": f"hethic{i}"})

    _write_snapshots("unified")

    with get_session() as sess:
        rows = sess.execute(text(
            "SELECT context, source_id, doc_count FROM context_snapshots "
            "WHERE bot='unified' ORDER BY context, source_id"
        )).fetchall()
    assert rows == [
        ("ethics_decisions", "*", 3),
        ("ethics_decisions", "pdf_ethics", 3),
        ("legal_text", "*", 3),
        ("legal_text", "חוק_הכנסת", 2),
        ("legal_text", "תקנון_הכנסת", 1),
    ], f"unexpected rows: {rows}"
