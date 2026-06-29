import hashlib
import json
import os
import subprocess
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text

from botnim.vector_store.vector_store_aurora import _normalize_law_name, _build_metadata_filter_sql, _LAW_NAME_NORM_SQL


def test_normalize_collapses_maqaf_colon_and_whitespace():
    # model emits hyphen+colon; stored basic laws use maqaf+colon — both must normalize equal
    assert _normalize_law_name("חוק-יסוד: הממשלה") == "חוק-יסוד הממשלה"
    assert _normalize_law_name("חוק־יסוד: הכנסת") == "חוק-יסוד הכנסת"   # maqaf U+05BE → hyphen
    assert _normalize_law_name("חוק-יסוד הממשלה") == "חוק-יסוד הממשלה"   # already-normal stored value
    assert _normalize_law_name("חוק   חובת  המכרזים ") == "חוק חובת המכרזים"
    assert _normalize_law_name(None) is None


def test_normalize_gershayim_and_geresh():
    # gershayim ״ (U+05F4) → ASCII double-quote; geresh ׳ (U+05F3) → ASCII single-quote
    assert _normalize_law_name('חוק הגנת הצרכן״') == 'חוק הגנת הצרכן"'
    assert _normalize_law_name("חוק ס׳ 5") == "חוק ס' 5"


def test_build_filter_normalizes_law_name():
    sql, params = _build_metadata_filter_sql({"law_name": "חוק-יסוד: הממשלה"})
    assert "metadata->>'law_name'" in sql and ":law_norm" in sql
    assert params == {"law_norm": "חוק-יסוד הממשלה"}
    assert "@>" not in sql            # law_name uses normalized equality, not containment


def test_build_filter_keeps_containment_for_other_keys():
    sql, params = _build_metadata_filter_sql({"decision_number": "550"})
    assert "metadata @> CAST(:mfilter AS jsonb)" in sql
    assert params == {"mfilter": '{"decision_number": "550"}'}


def test_build_filter_empty():
    assert _build_metadata_filter_sql(None) == ("", {})
    assert _build_metadata_filter_sql({}) == ("", {})


_PARITY_CASES = ["חוק-יסוד: הממשלה", "חוק־יסוד: הכנסת", "חוק   חובת  המכרזים", "תקנון הכנסת", "חוק ס׳ 5 התש״ך"]


@pytest.mark.parametrize("raw", _PARITY_CASES)
def test_python_and_sql_normalize_match(raw, database_url):
    # Run the SQL normalize expression over a literal value, compare to Python.
    sql = "SELECT " + _LAW_NAME_NORM_SQL.replace("metadata->>'law_name'", ":v")
    eng = create_engine(database_url)
    with eng.connect() as c:
        got = c.execute(text(sql), {"v": raw}).scalar()
    assert got == _normalize_law_name(raw)


# ---------------------------------------------------------------------------
# Integration test: auto-fallback when metadata_filter yields 0 rows
# ---------------------------------------------------------------------------

_REPO_ROOT = Path(__file__).resolve().parent.parent


def _alembic_upgrade_filter(database_url: str) -> None:
    alembic_bin = str(_REPO_ROOT / ".venv" / "bin" / "alembic")
    env = os.environ.copy()
    env["DATABASE_URL"] = database_url
    subprocess.run(
        [alembic_bin, "--config", "alembic.ini", "upgrade", "head"],
        cwd=_REPO_ROOT, env=env, check=True, capture_output=True,
    )


@pytest.fixture
def aurora_db_filter(database_url, monkeypatch):
    """Aurora backend pointed at a fresh per-test DB with schema applied."""
    _alembic_upgrade_filter(database_url)
    monkeypatch.setenv("DATABASE_URL", database_url)
    monkeypatch.setenv("OPENAI_API_KEY_STAGING", "sk-test")
    from botnim.db import session as s
    s._engine = None
    yield database_url
    s._engine = None


def _seed_law_doc(database_url, context_id, content, metadata, embedding):
    eng = create_engine(database_url)
    with eng.connect() as conn:
        content_hash = hashlib.sha256(content.encode()).hexdigest()
        conn.execute(text(
            "INSERT INTO documents (context_id, content, content_hash, metadata, embedding) "
            "VALUES (:cid, :c, :h, CAST(:m AS jsonb), CAST(:e AS vector))"
        ), {"cid": context_id, "c": content, "h": content_hash,
            "m": json.dumps(metadata), "e": str(embedding)})
        conn.commit()


def test_filter_miss_falls_back_to_unfiltered(aurora_db_filter, database_url, monkeypatch):
    from botnim.vector_store.vector_store_aurora import VectorStoreAurora
    from botnim.vector_store.search_modes import DEFAULT_SEARCH_MODE

    class _FakeEmbed:
        def embed(self, text):
            return [1.0] * 1536
    monkeypatch.setattr(
        "botnim.vector_store.vector_store_aurora._get_embedding_client",
        lambda env: _FakeEmbed(),
    )

    store = VectorStoreAurora(config={"slug": "unified", "name": "Unified"},
                              config_dir=".", environment="staging")
    cid = store.get_or_create_vector_store({"slug": "testlaws"}, "testlaws", False)

    emb = [1.0] * 1536  # non-zero — zero vectors break pgvector cosine
    _seed_law_doc(database_url, cid,
                  "חוק חובת המכרזים שמירת דינים",
                  {"law_name": "חוק חובת המכרזים", "DocumentTitle": "חוק חובת המכרזים"},
                  emb)

    # A filter that matches NOTHING → must fall back to unfiltered and still
    # surface the law lexically (tsv carries DocumentTitle at weight A).
    res = store.search(
        context_name="testlaws",
        query_text="חוק חובת המכרזים",
        search_mode=DEFAULT_SEARCH_MODE,
        embedding=emb,
        num_results=5,
        metadata_filter={"law_name": "חוק שלא קיים בכלל"},
    )
    assert res["hits"]["hits"], "empty filtered result should have fallen back to unfiltered and returned the law"


def test_law_name_norm_index_is_used(aurora_db_filter, database_url):
    # Proves the migration's indexed expression is byte-identical to
    # _LAW_NAME_NORM_SQL: with seqscan disabled, the planner can only use the
    # index if the WHERE expression matches the index expression exactly.
    from sqlalchemy import create_engine, text
    from botnim.vector_store.vector_store_aurora import _LAW_NAME_NORM_SQL
    eng = create_engine(database_url)
    with eng.begin() as c:
        cid = c.execute(text(
            "INSERT INTO contexts (id, bot, name) VALUES (gen_random_uuid(), 'b', 'idxctx') "
            "ON CONFLICT (bot, name) DO UPDATE SET updated_at=now() RETURNING id")).scalar()
        emb = "[" + ",".join(["0.1"] * 1536) + "]"
        for i in range(3):
            c.execute(text(
                "INSERT INTO documents (id, context_id, content, content_hash, metadata, embedding) "
                "VALUES (gen_random_uuid(), :cid, 'x', :h, CAST(:m AS jsonb), CAST(:e AS vector)) "
                "ON CONFLICT (context_id, content_hash) DO NOTHING"),
                {"cid": str(cid), "h": "idxh%d" % i,
                 "m": '{"law_name": "תקנון הכנסת", "DocumentTitle": "תקנון הכנסת"}', "e": emb})
    with eng.begin() as c:
        c.execute(text("SET LOCAL enable_seqscan = off"))
        plan = "\n".join(r[0] for r in c.execute(text(
            "EXPLAIN SELECT id FROM documents WHERE context_id = :cid "
            "AND metadata ? 'law_name' AND " + _LAW_NAME_NORM_SQL + " = :v"),
            {"cid": str(cid), "v": "תקנון הכנסת"}))
    # Verifies BOTH byte-identity (the WHERE expression matches the index expression)
    # AND that the partial predicate `metadata ? 'law_name'` is in the query — without
    # it PostgreSQL cannot prove the partial-index predicate and won't use the index.
    assert "documents_law_name_norm" in plan, plan


def test_scoped_vector_knn_sql_is_materialized_and_full_filter():
    from botnim.vector_store.vector_store_aurora import _scoped_vector_knn_sql, _LAW_NAME_NORM_SQL
    sql = _scoped_vector_knn_sql(" AND metadata @> CAST(:mfilter AS jsonb)")
    assert "AS MATERIALIZED" in sql                                  # mandatory keyword
    assert "metadata ? 'law_name'" in sql                           # partial-index predicate guard
    assert _LAW_NAME_NORM_SQL + " = :law_norm" in sql                # scoped by normalized law_name
    assert "metadata @> CAST(:mfilter AS jsonb)" in sql              # full filter (rest keys)
    assert "ORDER BY embedding <=> CAST(:emb AS vector)" in sql      # exact KNN
    assert _scoped_vector_knn_sql("").count("AS MATERIALIZED") == 1  # also present with empty rest


def test_scoped_vector_knn_hard_scopes_and_orders_by_distance(aurora_db_filter, database_url):
    from sqlalchemy import text
    from botnim.db.session import get_engine
    from botnim.vector_store.vector_store_aurora import _scoped_vector_knn, _build_metadata_filter_sql, _normalize_law_name
    eng = get_engine()
    with eng.begin() as c:
        cid = c.execute(text(
            "INSERT INTO contexts (id, bot, name) VALUES (gen_random_uuid(), 'b', 'sctx') "
            "ON CONFLICT (bot, name) DO UPDATE SET updated_at=now() RETURNING id")).scalar()

        def seed(h, law, emb_val):
            emb = "[" + ",".join([str(emb_val)] * 1536) + "]"
            c.execute(text(
                "INSERT INTO documents (id, context_id, content, content_hash, metadata, embedding) "
                "VALUES (gen_random_uuid(), :cid, 'c', :h, CAST(:m AS jsonb), CAST(:e AS vector))"),
                {"cid": str(cid), "h": h,
                 "m": '{"law_name": "%s", "DocumentTitle": "%s"}' % (law, law), "e": emb})
        seed("near", "תקנון הכנסת", 1.0)   # nearest to query emb [1.0]*1536 (distance 0)
        seed("far",  "תקנון הכנסת", -1.0)  # same law, far
        seed("other", "חוק אחר", 1.0)      # different law, also near — must be EXCLUDED
    rest_sql, rest_params = _build_metadata_filter_sql({})  # no non-law_name keys -> ("", {})
    q_emb = [1.0] * 1536
    with eng.connect() as c:
        rows = _scoped_vector_knn(c, str(cid), _normalize_law_name("תקנון הכנסת"),
                                  rest_sql, rest_params, q_emb, 10)
    import json as _json
    def _md(r):  # JSONB may come back as dict or str depending on driver registration
        return r[2] if isinstance(r[2], dict) else _json.loads(r[2])
    law_names = [_md(r)["law_name"] for r in rows]
    assert law_names and set(law_names) == {"תקנון הכנסת"}, law_names   # hard-scoped, no cross-law
    # nearest-first: the [1.0] doc (distance 0) outranks the [-1.0] doc (both same law)
    assert rows[0][3] > rows[-1][3]
