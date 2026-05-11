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
        # 2 legal_text docs from חוק_הכנסת + 1 from תקנון_הכנסת + 3 ethics docs.
        # `metadata->>'title'` must be unique per doc — that's what
        # _write_snapshots counts. Real per-row CSV contexts always set title.
        for src, n in [("חוק_הכנסת", 2), ("תקנון_הכנסת", 1)]:
            for i in range(n):
                sess.execute(text(
                    "INSERT INTO documents (context_id, content, content_hash, source_id, metadata) "
                    "VALUES (:c, :body, :h, :s, jsonb_build_object('title', CAST(:t AS text)))"
                ), {"c": cid_legal, "body": f"x{i}", "h": f"h{src}{i}",
                    "s": src, "t": f"{src}_section_{i}"})
        for i in range(3):
            sess.execute(text(
                "INSERT INTO documents (context_id, content, content_hash, source_id, metadata) "
                "VALUES (:c, :body, :h, 'pdf_ethics', jsonb_build_object('title', CAST(:t AS text)))"
            ), {"c": cid_eth, "body": f"e{i}", "h": f"hethic{i}", "t": f"ethics_decision_{i}"})

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


def test_write_snapshots_counts_distinct_source_doc_for_fanout_contexts(database_url, monkeypatch):
    """Fan-out contexts (knesset_protocols, plenary_schedule) split one
    upstream document into many embedding chunks. Each chunk gets a unique
    `title` (e.g. 'turn_42.md') but they share `metadata->>'source_doc'`.
    doc_count should reflect distinct source_doc, NOT chunk count, so the
    count an operator sees on /admin/sources matches the number of upstream
    files actually in the corpus. Mirrors LibreChat dec13ba76 test fixture.
    """
    _alembic_upgrade_head(database_url)
    monkeypatch.setenv("DATABASE_URL", database_url)
    import botnim.db.session
    botnim.db.session._engine = None
    botnim.db.session._SessionFactory = None

    with get_session() as sess:
        cid = sess.execute(text(
            "INSERT INTO contexts (bot, name) "
            "VALUES ('unified', 'knesset_protocols') RETURNING id"
        )).scalar()
        # 5 chunks across 2 source docs — each chunk has its own per-turn
        # title but they only represent 2 actual upstream .doc files.
        # Old code (count(*)) would say 5; new code says 2.
        chunks = [
            ("turn_1.md", "a.doc"),
            ("turn_2.md", "a.doc"),
            ("turn_3.md", "a.doc"),
            ("turn_4.md", "b.doc"),
            ("turn_5.md", "b.doc"),
        ]
        for i, (title, src_doc) in enumerate(chunks):
            sess.execute(text(
                "INSERT INTO documents (context_id, content, content_hash, source_id, metadata) "
                "VALUES (:c, :body, :h, 'protocols_csv', "
                "jsonb_build_object('title', CAST(:t AS text), 'source_doc', CAST(:s AS text)))"
            ), {"c": cid, "body": f"chunk{i}", "h": f"hp{i}", "t": title, "s": src_doc})

    _write_snapshots("unified")

    with get_session() as sess:
        rows = sess.execute(text(
            "SELECT context, source_id, doc_count FROM context_snapshots "
            "WHERE bot='unified' ORDER BY source_id"
        )).fetchall()
    # 2 distinct source_doc values (a.doc, b.doc), NOT 5 chunks and NOT 5
    # distinct titles. Both the per-source row and the '*' aggregate row
    # must reflect that.
    assert rows == [
        ("knesset_protocols", "*", 2),
        ("knesset_protocols", "protocols_csv", 2),
    ], f"unexpected rows: {rows}"


def test_write_snapshots_excludes_docs_missing_both_source_doc_and_title(database_url, monkeypatch):
    """Documents with neither source_doc nor title are excluded from the
    count (we have no identifier to dedupe by). This is operator-visible
    drift: it'll show up as a missing or zero count on /admin/sources and
    indicates an extractor that forgot to emit one of those keys.
    """
    _alembic_upgrade_head(database_url)
    monkeypatch.setenv("DATABASE_URL", database_url)
    import botnim.db.session
    botnim.db.session._engine = None
    botnim.db.session._SessionFactory = None

    with get_session() as sess:
        cid = sess.execute(text(
            "INSERT INTO contexts (bot, name) "
            "VALUES ('unified', 'broken_extractor') RETURNING id"
        )).scalar()
        # 3 docs with title (counted), 2 docs with neither (skipped).
        for i in range(3):
            sess.execute(text(
                "INSERT INTO documents (context_id, content, content_hash, source_id, metadata) "
                "VALUES (:c, :body, :h, 'src1', jsonb_build_object('title', CAST(:t AS text)))"
            ), {"c": cid, "body": f"ok{i}", "h": f"hok{i}", "t": f"good_{i}"})
        for i in range(2):
            sess.execute(text(
                "INSERT INTO documents (context_id, content, content_hash, source_id, metadata) "
                "VALUES (:c, :body, :h, 'src1', jsonb_build_object('other_key', 'x'))"
            ), {"c": cid, "body": f"bad{i}", "h": f"hbad{i}"})

    _write_snapshots("unified")

    with get_session() as sess:
        rows = sess.execute(text(
            "SELECT context, source_id, doc_count FROM context_snapshots "
            "WHERE bot='unified' ORDER BY source_id"
        )).fetchall()
    assert rows == [
        ("broken_extractor", "*", 3),
        ("broken_extractor", "src1", 3),
    ], f"unexpected rows: {rows}"


def test_sync_agents_calls_write_snapshots_on_success(monkeypatch):
    """Verify sync_agents invokes _write_snapshots once after iterating bots."""
    from unittest.mock import patch

    with patch("botnim.sync._sync_vector_store") as mock_sync, \
         patch("botnim.sync.publish_bot") as mock_publish, \
         patch("botnim.sync._write_snapshots") as mock_snapshots:
        from botnim.sync import sync_agents
        # Use 'unified' (only spec'd bot) so the SPECS.glob matches one config.
        sync_agents("staging", "unified", backend="aurora")
    assert mock_snapshots.called, "expected _write_snapshots to be called"
    assert mock_snapshots.call_args.args == ("unified",)
