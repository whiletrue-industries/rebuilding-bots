"""Verify the one-shot backfill SQL produces correct source_id assignments."""
import os
import subprocess
from pathlib import Path

from sqlalchemy import text

from botnim.db.session import get_session


REPO_ROOT = Path(__file__).resolve().parent.parent
BACKFILL_SQL = REPO_ROOT / "botnim" / "db" / "migrations" / "data" / "0005_backfill_source_id.sql"


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


def test_backfill_sets_source_id_on_legacy_rows(database_url, monkeypatch):
    _alembic_upgrade_head(database_url)
    monkeypatch.setenv("DATABASE_URL", database_url)
    import botnim.db.session as s
    s._engine = None
    s._SessionFactory = None

    with get_session() as sess:
        cid_govt = sess.execute(text(
            "INSERT INTO contexts (bot, name) VALUES ('unified', 'government_decisions') RETURNING id"
        )).scalar()
        cid_legal = sess.execute(text(
            "INSERT INTO contexts (bot, name) VALUES ('unified', 'legal_text') RETURNING id"
        )).scalar()
        # Single-source context — backfill should fill all rows with 'bk_csv'.
        for i in range(3):
            sess.execute(text(
                "INSERT INTO documents (context_id, content, content_hash) "
                "VALUES (:c, :b, :h)"
            ), {"c": cid_govt, "b": f"row{i}", "h": f"hgov{i}"})
        # Multi-source legal_text — backfill derives from '# <name>' header.
        sess.execute(text(
            "INSERT INTO documents (context_id, content, content_hash) "
            "VALUES (:c, '# חוק_הכנסת\n\nbody', :h)"
        ), {"c": cid_legal, "h": "hch1"})
        sess.execute(text(
            "INSERT INTO documents (context_id, content, content_hash) "
            "VALUES (:c, '# תקנון_הכנסת\n\nbody', :h)"
        ), {"c": cid_legal, "h": "htk1"})

    sql = BACKFILL_SQL.read_text()
    with get_session() as sess:
        for stmt in [s for s in sql.split(";") if s.strip() and not s.strip().startswith("--")]:
            sess.execute(text(stmt))

    with get_session() as sess:
        rows = sess.execute(text(
            "SELECT c.name, d.source_id, count(*) "
            "FROM documents d JOIN contexts c ON c.id=d.context_id "
            "WHERE c.bot='unified' GROUP BY c.name, d.source_id ORDER BY 1, 2"
        )).fetchall()
    assert rows == [
        ("government_decisions", "bk_csv", 3),
        ("legal_text", "חוק_הכנסת", 1),
        ("legal_text", "תקנון_הכנסת", 1),
    ], f"unexpected: {rows}"
