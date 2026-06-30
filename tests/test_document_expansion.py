import os
import subprocess
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text

# `database_url` is a conftest fixture (auto-available). Define a SELF-CONTAINED
# per-test-DB fixture here (same mechanism as tests/test_law_name_filter.py) rather
# than importing it cross-module, which is brittle across pytest import modes.
_REPO_ROOT = Path(__file__).resolve().parent.parent


@pytest.fixture
def aurora_db_filter(database_url, monkeypatch):
    alembic_bin = str(_REPO_ROOT / ".venv" / "bin" / "alembic")
    env = os.environ.copy()
    env["DATABASE_URL"] = database_url
    subprocess.run([alembic_bin, "--config", "alembic.ini", "upgrade", "head"],
                   cwd=_REPO_ROOT, env=env, check=True, capture_output=True)
    monkeypatch.setenv("DATABASE_URL", database_url)
    monkeypatch.setenv("OPENAI_API_KEY_STAGING", "sk-test")
    from botnim.db import session as s
    s._engine = None
    yield database_url
    s._engine = None


def _seed_chunk(database_url, cid, content, title, chunk_index, embedding="[" + ",".join(["0.1"] * 1536) + "]"):
    eng = create_engine(database_url)
    with eng.begin() as c:
        c.execute(text(
            "INSERT INTO documents (id, context_id, content, content_hash, metadata, embedding) "
            "VALUES (gen_random_uuid(), :cid, :content, :h, CAST(:m AS jsonb), CAST(:e AS vector))"),
            {"cid": cid, "content": content, "h": "%s#%d" % (title, chunk_index),
             "m": '{"DocumentTitle": "%s", "chunk_index": %d}' % (title, chunk_index), "e": embedding})


def _ctx(database_url, name="dctx"):
    eng = create_engine(database_url)
    with eng.begin() as c:
        return str(c.execute(text(
            "INSERT INTO contexts (id, bot, name) VALUES (gen_random_uuid(), 'b', :n) "
            "ON CONFLICT (bot, name) DO UPDATE SET updated_at=now() RETURNING id"), {"n": name}).scalar())


def _hit(title, content, score=1.0):
    return {"_id": "x", "_score": score, "_source": {"content": content, "metadata": {"DocumentTitle": title}}}


def test_expand_merges_full_decision(aurora_db_filter, database_url):
    from botnim.vector_store.vector_store_aurora import _expand_to_documents
    cid = _ctx(database_url)
    _seed_chunk(database_url, cid, "הכלל: היושב ראש רשאי להוציא לאחר שלוש קריאות לסדר", "החלטה א", 0)
    _seed_chunk(database_url, cid, "המשך נימוקים", "החלטה א", 1)
    _seed_chunk(database_url, cid, "אולם הקריאות לא תיעשנה באופן רצוף — תינתן שהות לתקן", "החלטה א", 3)
    eng = create_engine(database_url)
    with eng.connect() as c:
        # a single fragment-hit (chunk 0) -> expanded passage contains chunk 3's qualifier
        out = _expand_to_documents(c, cid, [_hit("החלטה א", "הכלל: היושב ראש רשאי להוציא לאחר שלוש קריאות לסדר")])
    assert len(out) == 1
    assert "לא תיעשנה באופן רצוף" in out[0]["_source"]["content"]
    assert out[0]["_source"]["metadata"]["_expanded_chunks"] == 3


def test_expand_dedups_same_title(aurora_db_filter, database_url):
    from botnim.vector_store.vector_store_aurora import _expand_to_documents
    cid = _ctx(database_url)
    _seed_chunk(database_url, cid, "chunk0", "החלטה ב", 0)
    _seed_chunk(database_url, cid, "chunk1", "החלטה ב", 1)
    eng = create_engine(database_url)
    with eng.connect() as c:
        out = _expand_to_documents(c, cid, [_hit("החלטה ב", "chunk0", 0.9), _hit("החלטה ב", "chunk1", 0.5)])
    assert len(out) == 1                       # two fragment-hits of one decision -> one result
    assert out[0]["_score"] == 0.9             # keeps the highest-ranked occurrence


def test_expand_passthrough_without_title(aurora_db_filter, database_url):
    from botnim.vector_store.vector_store_aurora import _expand_to_documents
    cid = _ctx(database_url)
    eng = create_engine(database_url)
    h = {"_id": "x", "_score": 1.0, "_source": {"content": "no title here", "metadata": {}}}
    with eng.connect() as c:
        out = _expand_to_documents(c, cid, [h])
    assert out == [h]                          # untouched, no _expanded_chunks tag


def test_expand_caps_chunks(aurora_db_filter, database_url):
    from botnim.vector_store.vector_store_aurora import _expand_to_documents
    cid = _ctx(database_url)
    for i in range(6):
        _seed_chunk(database_url, cid, "c%d" % i, "החלטה ג", i)
    eng = create_engine(database_url)
    with eng.connect() as c:
        out = _expand_to_documents(c, cid, [_hit("החלטה ג", "c0")], max_chunks=3)
    assert out[0]["_source"]["metadata"]["_expanded_chunks"] == 3
    assert out[0]["_source"]["metadata"]["_expanded_truncated"] is True
