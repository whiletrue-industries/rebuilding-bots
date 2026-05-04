"""Tests for gov_il_decisions.aurora_writer using a real Postgres DB.

Mirrors the test pattern in tests/test_vector_store_aurora.py — use the
pytest_postgresql fixture to spin up a per-test DB, run alembic upgrade
to apply the schema, then call the writer functions directly.
"""
from __future__ import annotations

import hashlib
import os
import subprocess
from pathlib import Path

import pytest
from sqlalchemy import text


REPO_ROOT = Path(__file__).resolve().parents[3]


def _alembic_upgrade(database_url: str) -> None:
    env = os.environ.copy()
    env["DATABASE_URL"] = database_url
    # Prefer the alembic in the venv that's running pytest (matches the
    # pattern used in tests/test_vector_store_aurora.py — but explicit so
    # this file works when pytest is invoked from outside the venv shell).
    import sys
    venv_alembic = Path(sys.executable).parent / "alembic"
    alembic = str(venv_alembic) if venv_alembic.exists() else "alembic"
    subprocess.run(
        [alembic, "--config", "alembic.ini", "upgrade", "head"],
        cwd=REPO_ROOT, env=env, check=True, capture_output=True,
    )


@pytest.fixture
def aurora_db(database_url, monkeypatch):
    """Aurora backend pointed at a fresh per-test DB with schema applied."""
    _alembic_upgrade(database_url)
    monkeypatch.setenv("DATABASE_URL", database_url)
    monkeypatch.setenv("OPENAI_API_KEY_STAGING", "sk-test")
    from botnim.db import session as s
    s._engine = None
    return database_url


class _FakeEmbeddingClient:
    def __init__(self):
        self.call_count = 0

    def embed(self, text: str) -> list:
        self.call_count += 1
        h = hashlib.sha256(text.encode()).digest()
        return [(b / 255.0) for b in h] * 48  # 32 * 48 = 1536


@pytest.fixture
def fake_embedder(monkeypatch):
    fake = _FakeEmbeddingClient()
    monkeypatch.setattr(
        "botnim.document_parser.gov_il_decisions.aurora_writer._get_embedding_client",
        lambda env: fake,
    )
    return fake


def test_round_trip(aurora_db, fake_embedder):
    from botnim.document_parser.gov_il_decisions.aurora_writer import (
        existing_page_ids,
        get_or_create_context,
        write_decision,
    )
    from botnim.db.session import get_engine

    cid = get_or_create_context("unified", "government_decisions")
    assert isinstance(cid, str) and len(cid) == 36

    n = write_decision(
        cid,
        page_id="dec3994-2026",
        title="כותרת בדיקה",
        text="גוף החלטה קצר",
        metadata={"action_type": "אחר", "domain": "כללי"},
        environment="staging",
    )
    assert n == 1

    # existing_page_ids reflects the new row
    pids = existing_page_ids(cid)
    assert pids == {"dec3994-2026"}

    # source_id is set on the row
    eng = get_engine()
    with eng.connect() as conn:
        row = conn.execute(text(
            "SELECT source_id, metadata FROM documents WHERE context_id=:cid"
        ), {"cid": cid}).fetchone()
    assert row[0] == "gov_il_decisions"


def test_chunking_produces_multiple_rows(aurora_db, fake_embedder):
    from botnim.document_parser.gov_il_decisions.aurora_writer import write_decision
    from botnim.db.session import get_engine
    from botnim.document_parser.gov_il_decisions.aurora_writer import (
        get_or_create_context,
    )

    cid = get_or_create_context("unified", "government_decisions")

    # Build text > CHUNK_MAX_TOKENS (6000) using varied content so each
    # chunk has a unique content_hash (a single repeated word produces
    # byte-identical overlap chunks that ON CONFLICT would dedup).
    long_body_parts = []
    for i in range(8000):
        long_body_parts.append(f"מילה{i}")
    long_body = " ".join(long_body_parts)

    n = write_decision(
        cid,
        page_id="dec-long-1",
        title="long",
        text=long_body,
        metadata={"action_type": "אחר", "domain": "כללי"},
        environment="staging",
    )
    assert n >= 2  # multiple chunks

    eng = get_engine()
    with eng.connect() as conn:
        rows = conn.execute(text(
            "SELECT metadata FROM documents WHERE context_id=:cid"
        ), {"cid": cid}).fetchall()
    assert len(rows) == n
    page_ids = {r[0]["page_id"] for r in rows}
    assert page_ids == {"dec-long-1"}
    # All chunks share the same total_chunks value, equal to len(rows)
    # since each chunk produced a unique row here.
    totals = {r[0]["total_chunks"] for r in rows}
    assert totals == {n}
    # chunk_index is a contiguous range starting at 0
    chunk_indexes = sorted(r[0]["chunk_index"] for r in rows)
    assert chunk_indexes == list(range(n))


def test_idempotent_on_rerun(aurora_db, fake_embedder):
    from botnim.document_parser.gov_il_decisions.aurora_writer import (
        get_or_create_context,
        write_decision,
    )
    from botnim.db.session import get_engine

    cid = get_or_create_context("unified", "government_decisions")

    kwargs = dict(
        page_id="dec-idemp-1",
        title="t",
        text="some body text",
        metadata={"action_type": "אחר", "domain": "כללי"},
        environment="staging",
    )
    n1 = write_decision(cid, **kwargs)
    assert n1 == 1

    n2 = write_decision(cid, **kwargs)
    assert n2 == 0  # ON CONFLICT DO NOTHING — no new rows

    eng = get_engine()
    with eng.connect() as conn:
        total = conn.execute(text(
            "SELECT count(*) FROM documents WHERE context_id=:cid"
        ), {"cid": cid}).scalar()
    assert total == 1


# ---------------------------------------------------------------------------
# write_decisions_batched — bulk path used by the Excel bootstrap.
# ---------------------------------------------------------------------------


class _FakeBatchOpenAI:
    """Mimics the subset of openai.OpenAI used by write_decisions_batched.

    Returns deterministic 1536-dim embeddings keyed off the input text's
    sha256 digest so each call is order-preserving and content-distinct.
    """

    def __init__(self):
        self.batch_calls: list[list[str]] = []
        self.embeddings = self  # let `client.embeddings.create(...)` work

    def create(self, *, input, model):
        # OpenAI accepts either str or list[str]; the batched path always
        # passes a list, so assert that and record it for later inspection.
        assert isinstance(input, list)
        self.batch_calls.append(list(input))

        class _Datum:
            def __init__(self, embedding):
                self.embedding = embedding

        class _Resp:
            def __init__(self, data):
                self.data = data

        data = []
        for txt in input:
            h = hashlib.sha256(txt.encode("utf-8")).digest()
            vec = [(b / 255.0) for b in h] * 48  # 32 * 48 = 1536
            data.append(_Datum(vec))
        return _Resp(data)


@pytest.fixture
def fake_batch_openai(monkeypatch):
    fake = _FakeBatchOpenAI()
    # The implementation constructs `openai.OpenAI(api_key=...)` directly to
    # access the multi-input embeddings API. Patch the symbol it imports.
    monkeypatch.setattr(
        "botnim.document_parser.gov_il_decisions.aurora_writer.OpenAI",
        lambda api_key=None: fake,
    )
    return fake


def test_batched_round_trip(aurora_db, fake_batch_openai):
    from botnim.document_parser.gov_il_decisions.aurora_writer import (
        get_or_create_context,
        write_decisions_batched,
    )
    from botnim.db.session import get_engine

    cid = get_or_create_context("unified", "government_decisions")

    # Build one short decision and one decision long enough to chunk.
    long_body_parts = [f"מילה{i}" for i in range(8000)]
    long_body = " ".join(long_body_parts)

    records = [
        {
            "context_id": cid,
            "page_id": "dec-a",
            "title": "כותרת א",
            "text": "גוף קצר א",
            "metadata": {"action_type": "אחר", "domain": "כללי"},
        },
        {
            "context_id": cid,
            "page_id": "dec-b",
            "title": "כותרת ב",
            "text": "גוף קצר ב",
            "metadata": {"action_type": "אחר", "domain": "כללי"},
        },
        {
            "context_id": cid,
            "page_id": "dec-long",
            "title": "ארוכה",
            "text": long_body,
            "metadata": {"action_type": "אחר", "domain": "כללי"},
        },
    ]
    result = write_decisions_batched(records, environment="staging")

    assert result["decisions"] == 3
    assert result["chunks_planned"] >= 4  # 1 + 1 + (>=2 from the long one)
    assert result["chunks_written"] == result["chunks_planned"]

    # Verify the long decision produced multiple rows with proper indexing
    eng = get_engine()
    with eng.connect() as conn:
        long_rows = conn.execute(text(
            "SELECT metadata FROM documents "
            "WHERE context_id=:cid AND metadata->>'page_id'=:pid"
        ), {"cid": cid, "pid": "dec-long"}).fetchall()
    assert len(long_rows) >= 2
    totals = {r[0]["total_chunks"] for r in long_rows}
    assert totals == {len(long_rows)}
    chunk_indexes = sorted(r[0]["chunk_index"] for r in long_rows)
    assert chunk_indexes == list(range(len(long_rows)))

    # source_id is set
    with eng.connect() as conn:
        srcs = {r[0] for r in conn.execute(text(
            "SELECT DISTINCT source_id FROM documents WHERE context_id=:cid"
        ), {"cid": cid}).fetchall()}
    assert srcs == {"gov_il_decisions"}

    # The OpenAI client was called in batched form (one call per
    # embedding_batch_size group) with the planned chunk count summing
    # across all calls.
    total_inputs = sum(len(c) for c in fake_batch_openai.batch_calls)
    assert total_inputs == result["chunks_planned"]


def test_batched_idempotent(aurora_db, fake_batch_openai):
    from botnim.document_parser.gov_il_decisions.aurora_writer import (
        get_or_create_context,
        write_decisions_batched,
    )
    from botnim.db.session import get_engine

    cid = get_or_create_context("unified", "government_decisions")
    records = [
        {
            "context_id": cid,
            "page_id": "dec-x",
            "title": "x",
            "text": "body x",
            "metadata": {"action_type": "אחר", "domain": "כללי"},
        },
        {
            "context_id": cid,
            "page_id": "dec-y",
            "title": "y",
            "text": "body y",
            "metadata": {"action_type": "אחר", "domain": "כללי"},
        },
    ]
    r1 = write_decisions_batched(records, environment="staging")
    assert r1["chunks_written"] == 2

    r2 = write_decisions_batched(records, environment="staging")
    assert r2["chunks_planned"] == 2
    assert r2["chunks_written"] == 0  # ON CONFLICT DO NOTHING

    eng = get_engine()
    with eng.connect() as conn:
        total = conn.execute(text(
            "SELECT count(*) FROM documents WHERE context_id=:cid"
        ), {"cid": cid}).scalar()
    assert total == 2


def test_batched_handles_empty_text(aurora_db, fake_batch_openai):
    from botnim.document_parser.gov_il_decisions.aurora_writer import (
        get_or_create_context,
        write_decisions_batched,
    )
    from botnim.db.session import get_engine

    cid = get_or_create_context("unified", "government_decisions")
    result = write_decisions_batched(
        [{
            "context_id": cid,
            "page_id": "dec-empty",
            "title": "no body",
            "text": "",
            "metadata": {"action_type": "אחר", "domain": "כללי"},
        }],
        environment="staging",
    )
    assert result["decisions"] == 1
    assert result["chunks_written"] == 1

    eng = get_engine()
    with eng.connect() as conn:
        rows = conn.execute(text(
            "SELECT content, metadata FROM documents WHERE context_id=:cid"
        ), {"cid": cid}).fetchall()
    assert len(rows) == 1
    assert rows[0][0] == ""  # empty content row written
    assert rows[0][1]["page_id"] == "dec-empty"
    assert rows[0][1]["total_chunks"] == 1
    assert rows[0][1]["chunk_index"] == 0


def test_batched_respects_embedding_batch_size(aurora_db, fake_batch_openai):
    """With 5 chunks and batch_size=2, expect ceil(5/2)=3 OpenAI calls."""
    from botnim.document_parser.gov_il_decisions.aurora_writer import (
        get_or_create_context,
        write_decisions_batched,
    )

    cid = get_or_create_context("unified", "government_decisions")
    records = [
        {
            "context_id": cid,
            "page_id": f"dec-batch-{i}",
            "title": f"t{i}",
            "text": f"body {i}",
            "metadata": {"action_type": "אחר", "domain": "כללי"},
        }
        for i in range(5)
    ]
    result = write_decisions_batched(
        records, environment="staging", embedding_batch_size=2,
    )
    assert result["chunks_planned"] == 5
    assert result["chunks_written"] == 5
    # 3 batches of sizes [2, 2, 1]
    assert [len(c) for c in fake_batch_openai.batch_calls] == [2, 2, 1]
