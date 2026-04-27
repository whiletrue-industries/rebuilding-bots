"""Tests for VectorStoreAurora — mirrors VectorStoreES test shape."""
import hashlib
import io
import json
import os
import subprocess
from pathlib import Path

import pytest
from sqlalchemy import text


REPO_ROOT = Path(__file__).resolve().parent.parent


def _alembic_upgrade(database_url: str) -> None:
    env = os.environ.copy()
    env["DATABASE_URL"] = database_url
    subprocess.run(
        ["alembic", "--config", "alembic.ini", "upgrade", "head"],
        cwd=REPO_ROOT, env=env, check=True, capture_output=True,
    )


@pytest.fixture
def aurora_db(database_url, monkeypatch):
    """Aurora backend pointed at a fresh per-test DB with schema applied."""
    _alembic_upgrade(database_url)
    monkeypatch.setenv("DATABASE_URL", database_url)
    monkeypatch.setenv("OPENAI_API_KEY_STAGING", "sk-test")
    # reset cached engine
    from botnim.db import session as s
    s._engine = None
    return database_url


def test_init_with_environment(aurora_db):
    from botnim.vector_store.vector_store_aurora import VectorStoreAurora
    config = {"slug": "unified", "name": "Unified"}
    store = VectorStoreAurora(config=config, config_dir=".", environment="staging")
    assert store.environment == "staging"
    assert store.production is False


def test_init_rejects_missing_environment(aurora_db):
    from botnim.vector_store.vector_store_aurora import VectorStoreAurora
    with pytest.raises(ValueError, match="Environment must be explicitly specified"):
        VectorStoreAurora(config={"slug": "u"}, config_dir=".", environment=None)


def test_get_or_create_vector_store_returns_uuid_and_inserts(aurora_db):
    from botnim.vector_store.vector_store_aurora import VectorStoreAurora
    from botnim.db.session import get_engine

    config = {"slug": "unified", "name": "Unified"}
    store = VectorStoreAurora(config=config, config_dir=".", environment="staging")
    context_obj = {"slug": "legal_text"}

    cid = store.get_or_create_vector_store(context_obj, "legal_text", replace_context=False)

    # Returned value is a uuid string
    assert isinstance(cid, str)
    assert len(cid) == 36  # uuid

    # Row landed in contexts
    eng = get_engine()
    with eng.connect() as conn:
        row = conn.execute(text(
            "SELECT id, bot, name FROM contexts WHERE bot=:bot AND name=:name"
        ), {"bot": "unified", "name": "legal_text"}).fetchone()
    assert row is not None
    assert str(row[0]) == cid


def test_get_or_create_is_idempotent(aurora_db):
    from botnim.vector_store.vector_store_aurora import VectorStoreAurora
    config = {"slug": "unified", "name": "Unified"}
    store = VectorStoreAurora(config=config, config_dir=".", environment="staging")
    cid1 = store.get_or_create_vector_store({"slug": "x"}, "x", False)
    cid2 = store.get_or_create_vector_store({"slug": "x"}, "x", False)
    assert cid1 == cid2  # same row, not a duplicate


def test_get_or_create_replace_context_clears_documents(aurora_db):
    from botnim.vector_store.vector_store_aurora import VectorStoreAurora
    from botnim.db.session import get_engine

    config = {"slug": "unified", "name": "Unified"}
    store = VectorStoreAurora(config=config, config_dir=".", environment="staging")
    cid = store.get_or_create_vector_store({"slug": "x"}, "x", False)

    # Manually insert a document
    eng = get_engine()
    with eng.connect() as conn:
        conn.execute(text(
            "INSERT INTO documents (context_id, content, content_hash) "
            "VALUES (:cid, 'hello', 'h1')"
        ), {"cid": cid})
        conn.commit()

    # replace_context=True should preserve the context row but clear its docs
    cid2 = store.get_or_create_vector_store({"slug": "x"}, "x", replace_context=True)
    assert cid2 == cid
    with eng.connect() as conn:
        n = conn.execute(text(
            "SELECT count(*) FROM documents WHERE context_id=:cid"
        ), {"cid": cid}).scalar()
    assert n == 0


class _FakeEmbeddingClient:
    """Stand-in for the OpenAI client. Counts calls so we can assert
    content-hash skip is actually skipping embeds."""
    def __init__(self):
        self.call_count = 0

    def embed(self, text: str) -> list:
        self.call_count += 1
        # Deterministic fake embedding — just a hash spread over 1536 dims
        h = hashlib.sha256(text.encode()).digest()
        return [(b / 255.0) for b in h] * 48  # 32 * 48 = 1536


def _make_file_streams(items: list):
    """items: [(filename, content, metadata)]. Returns the (fname, file, type, metadata) tuple shape."""
    return [
        (fname, io.BytesIO(content.encode()), "md", metadata)
        for fname, content, metadata in items
    ]


def test_upload_files_inserts_new_documents(aurora_db, monkeypatch):
    from botnim.vector_store.vector_store_aurora import VectorStoreAurora
    from botnim.db.session import get_engine

    fake = _FakeEmbeddingClient()
    monkeypatch.setattr(
        "botnim.vector_store.vector_store_aurora._get_embedding_client",
        lambda env: fake,
    )

    config = {"slug": "unified", "name": "Unified"}
    store = VectorStoreAurora(config=config, config_dir=".", environment="staging")
    cid = store.get_or_create_vector_store({"slug": "x"}, "x", False)

    streams = _make_file_streams([
        ("a.md", "alpha content", {"title": "A"}),
        ("b.md", "beta content",  {"title": "B"}),
    ])
    callback_calls = []
    store.upload_files({"slug": "x"}, "x", cid, streams, callback_calls.append)

    eng = get_engine()
    with eng.connect() as conn:
        n = conn.execute(text(
            "SELECT count(*) FROM documents WHERE context_id=:cid"
        ), {"cid": cid}).scalar()
    assert n == 2
    assert fake.call_count == 2  # one embed per new document
    assert callback_calls == [2]


def test_upload_files_skips_unchanged_content(aurora_db, monkeypatch):
    """Re-uploading the same content must not call the embedding API."""
    from botnim.vector_store.vector_store_aurora import VectorStoreAurora

    fake = _FakeEmbeddingClient()
    monkeypatch.setattr(
        "botnim.vector_store.vector_store_aurora._get_embedding_client",
        lambda env: fake,
    )

    config = {"slug": "unified", "name": "Unified"}
    store = VectorStoreAurora(config=config, config_dir=".", environment="staging")
    cid = store.get_or_create_vector_store({"slug": "x"}, "x", False)

    streams = _make_file_streams([("a.md", "same content", {"title": "A"})])
    store.upload_files({"slug": "x"}, "x", cid, streams, lambda n: None)
    assert fake.call_count == 1

    # Second upload, same content: must skip embed entirely
    streams = _make_file_streams([("a.md", "same content", {"title": "A"})])
    store.upload_files({"slug": "x"}, "x", cid, streams, lambda n: None)
    assert fake.call_count == 1  # unchanged

    # Third upload, *different* content: re-embeds
    streams = _make_file_streams([("a.md", "different content", {"title": "A"})])
    store.upload_files({"slug": "x"}, "x", cid, streams, lambda n: None)
    assert fake.call_count == 2


def test_upload_files_skips_non_markdown(aurora_db, monkeypatch):
    from botnim.vector_store.vector_store_aurora import VectorStoreAurora
    from botnim.db.session import get_engine

    fake = _FakeEmbeddingClient()
    monkeypatch.setattr(
        "botnim.vector_store.vector_store_aurora._get_embedding_client",
        lambda env: fake,
    )

    config = {"slug": "unified", "name": "Unified"}
    store = VectorStoreAurora(config=config, config_dir=".", environment="staging")
    cid = store.get_or_create_vector_store({"slug": "x"}, "x", False)

    streams = _make_file_streams([("a.txt", "ignored", {})])
    store.upload_files({"slug": "x"}, "x", cid, streams, lambda n: None)

    eng = get_engine()
    with eng.connect() as conn:
        n = conn.execute(text("SELECT count(*) FROM documents")).scalar()
    assert n == 0
    assert fake.call_count == 0


def test_delete_existing_files_removes_by_filename(aurora_db, monkeypatch):
    from botnim.vector_store.vector_store_aurora import VectorStoreAurora
    from botnim.db.session import get_engine

    fake = _FakeEmbeddingClient()
    monkeypatch.setattr(
        "botnim.vector_store.vector_store_aurora._get_embedding_client",
        lambda env: fake,
    )

    config = {"slug": "unified", "name": "Unified"}
    store = VectorStoreAurora(config=config, config_dir=".", environment="staging")
    cid = store.get_or_create_vector_store({"slug": "x"}, "x", False)

    streams = _make_file_streams([
        ("keep.md", "kept", {}),
        ("drop.md", "dropped", {}),
    ])
    store.upload_files({"slug": "x"}, "x", cid, streams, lambda n: None)

    deleted = store.delete_existing_files({"slug": "x"}, cid, ["drop.md"])
    assert deleted == 1

    eng = get_engine()
    with eng.connect() as conn:
        rows = conn.execute(text(
            "SELECT (metadata->>'filename') FROM documents WHERE context_id=:cid"
        ), {"cid": cid}).fetchall()
    names = {r[0] for r in rows}
    assert names == {"keep.md"}


def _seed_documents(database_url, context_id, docs):
    """docs: [(content, embedding, metadata)]"""
    from sqlalchemy import create_engine
    eng = create_engine(database_url)
    with eng.connect() as conn:
        for content, embedding, metadata in docs:
            content_hash = hashlib.sha256(content.encode()).hexdigest()
            conn.execute(text(
                "INSERT INTO documents (context_id, content, content_hash, metadata, embedding) "
                "VALUES (:cid, :c, :h, CAST(:m AS jsonb), CAST(:e AS vector))"
            ), {
                "cid": context_id, "c": content, "h": content_hash,
                "m": json.dumps(metadata), "e": str(embedding),
            })
        conn.commit()


def test_search_returns_top_k_by_vector_similarity(aurora_db, database_url, monkeypatch):
    from botnim.vector_store.vector_store_aurora import VectorStoreAurora
    from botnim.vector_store.search_modes import DEFAULT_SEARCH_MODE

    fake = _FakeEmbeddingClient()
    monkeypatch.setattr(
        "botnim.vector_store.vector_store_aurora._get_embedding_client",
        lambda env: fake,
    )

    config = {"slug": "unified", "name": "Unified"}
    store = VectorStoreAurora(config=config, config_dir=".", environment="staging")
    cid = store.get_or_create_vector_store({"slug": "x"}, "x", False)

    # Three docs with hand-picked embeddings
    target_embedding = [1.0] * 1536
    near_embedding = [0.99] * 1536
    far_embedding = [-1.0] * 1536
    _seed_documents(database_url, cid, [
        ("matches well",      target_embedding, {"title": "match"}),
        ("close-ish",          near_embedding,  {"title": "near"}),
        ("totally different",  far_embedding,   {"title": "far"}),
    ])

    results = store.search(
        context_name="x",
        query_text="anything",
        search_mode=DEFAULT_SEARCH_MODE,
        embedding=target_embedding,
        num_results=2,
    )

    assert "hits" in results
    titles = [hit["_source"]["metadata"]["title"] for hit in results["hits"]["hits"]]
    assert titles[0] == "match"
    assert "far" not in titles


def test_search_combines_vector_and_text_via_rrf(aurora_db, database_url, monkeypatch):
    """A doc that's a perfect text match but mediocre vector match should
    still rank high thanks to RRF."""
    from botnim.vector_store.vector_store_aurora import VectorStoreAurora
    from botnim.vector_store.search_modes import DEFAULT_SEARCH_MODE

    fake = _FakeEmbeddingClient()
    monkeypatch.setattr(
        "botnim.vector_store.vector_store_aurora._get_embedding_client",
        lambda env: fake,
    )

    config = {"slug": "unified", "name": "Unified"}
    store = VectorStoreAurora(config=config, config_dir=".", environment="staging")
    cid = store.get_or_create_vector_store({"slug": "x"}, "x", False)

    query_embedding = [1.0] * 1536
    weak_match_embedding = [0.5] * 1536
    _seed_documents(database_url, cid, [
        ("the medical complaints commissioner is a public role",
         weak_match_embedding, {"title": "text-strong"}),
        ("unrelated content about budgets",
         query_embedding, {"title": "vector-strong"}),
    ])

    results = store.search(
        context_name="x",
        query_text="medical complaints commissioner",
        search_mode=DEFAULT_SEARCH_MODE,
        embedding=query_embedding,
        num_results=2,
    )

    titles = [hit["_source"]["metadata"]["title"] for hit in results["hits"]["hits"]]
    # Both should appear; ordering proves RRF combined them
    assert set(titles) == {"text-strong", "vector-strong"}


def test_search_respects_metadata_filter(aurora_db, database_url, monkeypatch):
    """The Aurora backend's search must filter by metadata jsonb when
    the search_mode requests it (mirroring ES's behavior)."""
    from botnim.vector_store.vector_store_aurora import VectorStoreAurora
    from botnim.vector_store.search_modes import DEFAULT_SEARCH_MODE

    fake = _FakeEmbeddingClient()
    monkeypatch.setattr(
        "botnim.vector_store.vector_store_aurora._get_embedding_client",
        lambda env: fake,
    )

    config = {"slug": "unified", "name": "Unified"}
    store = VectorStoreAurora(config=config, config_dir=".", environment="staging")
    cid = store.get_or_create_vector_store({"slug": "x"}, "x", False)

    e = [1.0] * 1536
    _seed_documents(database_url, cid, [
        ("doc A", e, {"category": "legal"}),
        ("doc B", e, {"category": "budget"}),
    ])

    results = store.search(
        context_name="x",
        query_text="anything",
        search_mode=DEFAULT_SEARCH_MODE,
        embedding=e,
        num_results=10,
        metadata_filter={"category": "legal"},
    )

    titles = [hit["_source"]["content"] for hit in results["hits"]["hits"]]
    assert titles == ["doc A"]


def test_update_tools_emits_function_definition(aurora_db):
    from botnim.vector_store.vector_store_aurora import VectorStoreAurora
    config = {"slug": "unified", "name": "Unified"}
    store = VectorStoreAurora(config=config, config_dir=".", environment="staging")
    cid = store.get_or_create_vector_store({"slug": "legal_text"}, "legal_text", False)

    store.update_tools({"slug": "legal_text", "description": "Legal Israeli law"}, "legal_text")
    assert len(store.tools) == 1
    fn = store.tools[0]["function"]
    assert fn["name"] == "search_legal_text"
    assert "Legal Israeli law" in fn["description"]
    assert "query" in fn["parameters"]["properties"]
    assert "search_mode" in fn["parameters"]["properties"]


def test_update_tool_resources_sets_none(aurora_db):
    from botnim.vector_store.vector_store_aurora import VectorStoreAurora
    config = {"slug": "unified", "name": "Unified"}
    store = VectorStoreAurora(config=config, config_dir=".", environment="staging")
    store.update_tool_resources({}, "anything")
    assert store.tool_resources is None


def test_upload_files_skips_files_that_fail_to_embed(aurora_db, monkeypatch):
    """A file whose embed() raises must NOT abort the whole batch — log + skip,
    continue with the next file. Mirrors VectorStoreES._upload_files_async's
    return_exceptions=True behavior.
    """
    from botnim.vector_store.vector_store_aurora import VectorStoreAurora
    from botnim.db.session import get_engine

    class _FlakyEmbeddingClient:
        def __init__(self):
            self.calls = 0
        def embed(self, text):
            self.calls += 1
            if "BOOM" in text:
                raise RuntimeError("simulated embedding failure")
            return [(self.calls % 256) / 255.0] * 1536

    fake = _FlakyEmbeddingClient()
    monkeypatch.setattr(
        "botnim.vector_store.vector_store_aurora._get_embedding_client",
        lambda env: fake,
    )

    config = {"slug": "unified", "name": "Unified"}
    store = VectorStoreAurora(config=config, config_dir=".", environment="staging")
    cid = store.get_or_create_vector_store({"slug": "x"}, "x", False)

    streams = _make_file_streams([
        ("good1.md", "fine content", {"title": "good1"}),
        ("bad.md",    "BOOM oversized content", {"title": "bad"}),
        ("good2.md", "also fine", {"title": "good2"}),
    ])
    callback_calls = []
    store.upload_files({"slug": "x"}, "x", cid, streams, callback_calls.append)

    eng = get_engine()
    with eng.connect() as conn:
        rows = conn.execute(text(
            "SELECT (metadata->>'filename') FROM documents WHERE context_id=:cid"
        ), {"cid": cid}).fetchall()
    names = sorted(r[0] for r in rows)
    assert names == ["good1.md", "good2.md"]
    assert callback_calls == [2]


# ---- chunking (oversize content handling) ----

def test_chunk_for_embedding_short_content_returns_single():
    from botnim.vector_store.vector_store_aurora import _chunk_for_embedding
    chunks = _chunk_for_embedding("hello world")
    assert chunks == ["hello world"]


def test_chunk_for_embedding_under_limit_returns_single():
    from botnim.vector_store.vector_store_aurora import _chunk_for_embedding, CHUNK_MAX_TOKENS
    # Roughly half the limit — must come back as a single chunk
    content = "paragraph one.\n\n" + ("word " * (CHUNK_MAX_TOKENS // 4))
    chunks = _chunk_for_embedding(content)
    assert len(chunks) == 1


def test_chunk_for_embedding_oversize_splits_at_section_boundaries():
    """Content with ## section markers, each within the limit, must split
    cleanly at the markers (not mid-section)."""
    from botnim.vector_store.vector_store_aurora import (
        _chunk_for_embedding,
        CHUNK_MAX_TOKENS,
    )
    # Build content of 3 sections, each ~3000 tokens (well under the limit
    # individually, but together over the limit so chunking must engage).
    section_words = " ".join(["alpha"] * 2500)
    content = (
        f"## Section 1\n{section_words}\n\n"
        f"## Section 2\n{section_words}\n\n"
        f"## Section 3\n{section_words}\n"
    )
    chunks = _chunk_for_embedding(content, max_tokens=CHUNK_MAX_TOKENS)
    assert len(chunks) >= 2
    # Each chunk under the limit
    import tiktoken
    enc = tiktoken.get_encoding("cl100k_base")
    for c in chunks:
        assert len(enc.encode(c)) <= CHUNK_MAX_TOKENS
    # Section markers preserved at chunk boundaries
    joined = "\n".join(chunks)
    assert "## Section 1" in joined
    assert "## Section 3" in joined


def test_chunk_for_embedding_no_section_markers_falls_back_to_paragraphs():
    """Content with no ## markers must split on paragraph boundaries."""
    from botnim.vector_store.vector_store_aurora import (
        _chunk_for_embedding,
        CHUNK_MAX_TOKENS,
    )
    para = "word " * 2500
    content = f"{para}\n\n{para}\n\n{para}\n\n{para}\n"
    chunks = _chunk_for_embedding(content, max_tokens=CHUNK_MAX_TOKENS)
    assert len(chunks) >= 2
    import tiktoken
    enc = tiktoken.get_encoding("cl100k_base")
    for c in chunks:
        assert len(enc.encode(c)) <= CHUNK_MAX_TOKENS


def test_chunk_for_embedding_single_oversized_section_hard_splits():
    """A single section that itself exceeds the limit must be hard-split
    by token windows (last-resort path)."""
    from botnim.vector_store.vector_store_aurora import (
        _chunk_for_embedding,
        CHUNK_MAX_TOKENS,
    )
    # One paragraph, 9000 tokens — single section, must be hard-split
    content = "## Only Section\n" + ("word " * 9000)
    chunks = _chunk_for_embedding(content, max_tokens=CHUNK_MAX_TOKENS)
    assert len(chunks) >= 2
    import tiktoken
    enc = tiktoken.get_encoding("cl100k_base")
    for c in chunks:
        assert len(enc.encode(c)) <= CHUNK_MAX_TOKENS


def test_upload_files_chunks_oversize_content_into_multiple_rows(aurora_db, monkeypatch):
    """An oversize file produces multiple `documents` rows, each with the
    same `metadata.filename` and distinct `chunk_index`/`total_chunks`. No
    skip-on-error happens — every chunk gets embedded and inserted."""
    from botnim.vector_store.vector_store_aurora import VectorStoreAurora
    from botnim.db.session import get_engine

    fake = _FakeEmbeddingClient()
    monkeypatch.setattr(
        "botnim.vector_store.vector_store_aurora._get_embedding_client",
        lambda env: fake,
    )

    config = {"slug": "unified", "name": "Unified"}
    store = VectorStoreAurora(config=config, config_dir=".", environment="staging")
    cid = store.get_or_create_vector_store({"slug": "x"}, "x", False)

    # 3-section doc, each section ~3000 tokens → must chunk into ≥2 rows
    section_words = " ".join(["alpha"] * 2500)
    long_content = (
        f"## Section 1\n{section_words}\n\n"
        f"## Section 2\n{section_words}\n\n"
        f"## Section 3\n{section_words}\n"
    )
    streams = _make_file_streams([
        ("oversize.md", long_content, {"title": "long"}),
        ("normal.md", "short content", {"title": "normal"}),
    ])
    store.upload_files({"slug": "x"}, "x", cid, streams, lambda n: None)

    eng = get_engine()
    with eng.connect() as conn:
        rows = conn.execute(text(
            "SELECT metadata FROM documents WHERE context_id=:cid ORDER BY (metadata->>\x27filename\x27), (metadata->>\x27chunk_index\x27)::int NULLS FIRST"
        ), {"cid": cid}).fetchall()

    metadatas = [r[0] if isinstance(r[0], dict) else json.loads(r[0]) for r in rows]
    oversize_rows = [m for m in metadatas if m["filename"] == "oversize.md"]
    normal_rows = [m for m in metadatas if m["filename"] == "normal.md"]

    # Oversize file produced ≥2 chunks, each tagged with chunk_index/total_chunks
    assert len(oversize_rows) >= 2
    assert all("chunk_index" in m for m in oversize_rows)
    assert all("total_chunks" in m for m in oversize_rows)
    indices = sorted(int(m["chunk_index"]) for m in oversize_rows)
    assert indices == list(range(len(oversize_rows)))
    total = oversize_rows[0]["total_chunks"]
    assert all(int(m["total_chunks"]) == total for m in oversize_rows)

    # Normal file produced exactly 1 row, no chunk_index/total_chunks
    # (backward-compat: single-chunk path doesn't add the chunk metadata)
    assert len(normal_rows) == 1
    assert "chunk_index" not in normal_rows[0]
    assert "total_chunks" not in normal_rows[0]


def test_upload_files_oversize_content_replaces_old_skip_behavior(aurora_db, monkeypatch):
    """The doc that USED to fail with `Invalid 'input': maximum context
    length is 8192 tokens` now uploads successfully via chunking. This is
    the regression check — previously upload_files raised, now it doesn't."""
    from botnim.vector_store.vector_store_aurora import VectorStoreAurora
    from botnim.db.session import get_engine

    fake = _FakeEmbeddingClient()
    monkeypatch.setattr(
        "botnim.vector_store.vector_store_aurora._get_embedding_client",
        lambda env: fake,
    )

    config = {"slug": "unified", "name": "Unified"}
    store = VectorStoreAurora(config=config, config_dir=".", environment="staging")
    cid = store.get_or_create_vector_store({"slug": "x"}, "x", False)

    # ~10000-token single-blob content (no section markers, no paragraph
    # breaks) — the worst case for chunking, hits the hard-token-split path.
    content = "alpha " * 10000
    streams = _make_file_streams([("biggie.md", content, {})])

    callback_count = []
    store.upload_files({"slug": "x"}, "x", cid, streams, callback_count.append)

    eng = get_engine()
    with eng.connect() as conn:
        n = conn.execute(text(
            "SELECT count(*) FROM documents WHERE context_id=:cid AND metadata->>\x27filename\x27=\x27biggie.md\x27"
        ), {"cid": cid}).scalar()
    assert n >= 2  # multiple chunks
    assert callback_count[0] >= 2  # callback got the row count, not the file count


def test_upload_files_writes_source_id_from_metadata(aurora_db, monkeypatch):
    """A file_stream tuple whose metadata carries source_id should result
    in documents.source_id being populated."""
    from botnim.vector_store.vector_store_aurora import VectorStoreAurora
    from botnim.db.session import get_engine

    fake = _FakeEmbeddingClient()
    monkeypatch.setattr(
        "botnim.vector_store.vector_store_aurora._get_embedding_client",
        lambda env: fake,
    )

    config = {"slug": "unified", "name": "Unified"}
    store = VectorStoreAurora(config=config, config_dir=".", environment="staging")
    cid = store.get_or_create_vector_store({"slug": "legal_text"}, "legal_text", False)

    streams = [(
        "doc1.md",
        io.BytesIO("# חוק_הכנסת\n\nbody text".encode("utf-8")),
        "text/markdown",
        {"title": "x", "source_id": "חוק_הכנסת"},
    )]
    store.upload_files({"slug": "legal_text"}, "legal_text", cid, streams, callback=None)

    eng = get_engine()
    with eng.connect() as conn:
        rows = conn.execute(text(
            "SELECT source_id FROM documents WHERE context_id = :cid"
        ), {"cid": cid}).fetchall()
    assert rows, "expected at least one row inserted"
    assert all(r[0] == "חוק_הכנסת" for r in rows), f"all rows should have source_id; got {rows!r}"
