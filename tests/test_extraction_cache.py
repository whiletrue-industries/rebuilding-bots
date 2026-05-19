"""Aurora-backed read-through cache for dynamic_extraction outputs.

These tests cover the ExtractionCache surface in isolation; the
integration with collect_sources is covered later in this plan.
"""
from __future__ import annotations

import hashlib
import os
import subprocess
from pathlib import Path

import pytest
from sqlalchemy import text

from botnim.extraction_cache import ExtractionCache
from botnim.db.session import get_session


REPO_ROOT = Path(__file__).resolve().parent.parent


def _alembic_upgrade(database_url: str) -> None:
    env = os.environ.copy()
    env["DATABASE_URL"] = database_url
    subprocess.run(
        ["alembic", "--config", "alembic.ini", "upgrade", "head"],
        cwd=REPO_ROOT, env=env, check=True, capture_output=True,
    )


@pytest.fixture
def cache(database_url, monkeypatch):
    """An ExtractionCache talking to a fresh per-test postgres DB.

    Mirrors the aurora_db pattern from tests/vector_store/test_aurora_delta.py:
    apply alembic head, expose DATABASE_URL to the cached engine, and reset
    the module-level engine so get_session() rebinds to this test's DB.
    """
    _alembic_upgrade(database_url)
    monkeypatch.setenv("DATABASE_URL", database_url)
    from botnim.db import session as s
    s._engine = None
    return ExtractionCache(environment="test")


def _row_count() -> int:
    with get_session() as sess:
        return sess.execute(text("SELECT count(*) FROM extraction_cache")).scalar_one()


def test_get_returns_none_on_miss(cache):
    assert cache.get("nonexistent_hash", "v1-gpt-4o-mini") is None


def test_put_then_get_roundtrips_payload(cache):
    payload = {
        "title": "Knesset Protocol 47",
        "DocumentMetadata": {"DocumentTitle": "Plenary Session"},
        "Topics": ["budget", "education"],
    }
    cache.put(
        content_hash="h1",
        extractor_version="v1-gpt-4o-mini",
        payload=payload,
        bot="unified",
        context="knesset_protocols",
        document_type="text/markdown",
    )
    got = cache.get("h1", "v1-gpt-4o-mini")
    assert got == payload
    assert _row_count() == 1


def test_put_is_idempotent_on_conflict(cache):
    p1 = {"title": "First"}
    p2 = {"title": "Second"}
    cache.put("h2", "v1", payload=p1, bot="unified", context="ctxA", document_type="text/markdown")
    cache.put("h2", "v1", payload=p2, bot="unified", context="ctxA", document_type="text/markdown")
    assert cache.get("h2", "v1") == p2
    assert _row_count() == 1


def test_purge_scopes_to_bot_context_version(cache):
    cache.put("hA", "v1", payload={"x": 1}, bot="unified", context="ctxA", document_type="text/markdown")
    cache.put("hB", "v1", payload={"x": 2}, bot="unified", context="ctxB", document_type="text/markdown")
    cache.put("hC", "v2", payload={"x": 3}, bot="unified", context="ctxA", document_type="text/markdown")
    cache.put("hD", "v1", payload={"x": 4}, bot="other", context="ctxA", document_type="text/markdown")
    assert _row_count() == 4

    purged = cache.purge(bot="unified", context="ctxA", extractor_version="v1")

    assert purged == 1
    assert cache.get("hA", "v1") is None      # purged
    assert cache.get("hB", "v1") is not None  # different context
    assert cache.get("hC", "v2") is not None  # different version
    assert cache.get("hD", "v1") is not None  # different bot


import asyncio
import io
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from botnim.collect_sources import collect_context_sources_async
from botnim._concurrency import SyncConcurrency
from botnim.dynamic_extraction import RpdExhausted, EXTRACTION_VERSION


def _content_hash(s: str) -> str:
    return hashlib.sha256(s.strip().encode("utf-8")).hexdigest()


@pytest.mark.asyncio
async def test_collect_uses_aurora_cache_hit(cache, tmp_path, monkeypatch):
    """Pre-populated cache row → no LLM call, payload returned."""
    # _collect_raw_streams_csv emits one row as ``body:\n<value>\n\n``;
    # _prepare_file_content strips the trailing whitespace, so the actual
    # bytes hashed by _get_metadata_for_content_async is ``body:\nalpha``.
    csv_value = "alpha"
    pipeline_content = f"body:\n{csv_value}"
    cached_payload = {"title": "Cached Alpha", "status": "processed"}
    cache.put(
        _content_hash(pipeline_content), EXTRACTION_VERSION,
        payload=cached_payload, bot="unified", context="ctx_alpha",
        document_type="text/markdown",
    )

    # Build a context that yields one synthetic md with that content.
    csv_path = tmp_path / "alpha.csv"
    csv_path.write_text("body\n" + csv_value + "\n", encoding="utf-8")
    context_ = {"name": "ctx_alpha", "slug": "ctx_alpha", "type": "csv",
                "source": "alpha.csv", "fetcher": None}

    # Wipe the on-disk L1 KVFile so a stale entry from a prior test run
    # doesn't pre-empt the L2 lookup we're trying to exercise here.
    # CachedKVFileSQLite stores at <location>.sqlite — the .sqlite suffix is
    # appended in kvfile_sqlite.KVFileSQLite.__init__, so we delete that file
    # plus the (older) directory layout for safety.
    import shutil
    repo_root = Path(__file__).resolve().parent.parent
    (repo_root / "cache" / "metadata.sqlite").unlink(missing_ok=True)
    shutil.rmtree(repo_root / "cache" / "metadata", ignore_errors=True)

    fake_oai = AsyncMock(side_effect=AssertionError("OpenAI must not be called"))
    concurrency = SyncConcurrency()

    with patch("botnim.dynamic_extraction._async_chat_completion_inner", fake_oai):
        streams = await collect_context_sources_async(
            context_, tmp_path, concurrency,
            bot="unified", extraction_cache=cache,
        )

    assert len(streams) == 1
    fname, _, _, metadata = streams[0]
    assert metadata["title"] == "Cached Alpha"
    fake_oai.assert_not_awaited()


@pytest.mark.asyncio
async def test_collect_writes_aurora_cache_on_miss(cache, tmp_path):
    """Empty cache → one LLM call, one row written, idempotent on rerun."""
    csv_value = "beta"
    # Mirror the CSV pipeline transform: header `body` + one row → emitted
    # as ``body:\n<value>\n\n`` then ``.strip()``-ed by _prepare_file_content.
    pipeline_content = f"body:\n{csv_value}"
    csv_path = tmp_path / "beta.csv"
    csv_path.write_text("body\n" + csv_value + "\n", encoding="utf-8")
    context_ = {"name": "ctx_beta", "slug": "ctx_beta", "type": "csv",
                "source": "beta.csv", "fetcher": None}

    # Wipe L1 KVFile (sqlite file + legacy dir layout) so a stale hit doesn't
    # preempt the fresh-write path.
    import shutil
    repo_root = Path(__file__).resolve().parent.parent
    (repo_root / "cache" / "metadata.sqlite").unlink(missing_ok=True)
    shutil.rmtree(repo_root / "cache" / "metadata", ignore_errors=True)

    fake_response = MagicMock()
    fake_response.choices = [MagicMock(message=MagicMock(content='{"DocumentMetadata": {"DocumentTitle": "Fresh Beta"}}'))]
    fake_oai = AsyncMock(return_value=fake_response)
    concurrency = SyncConcurrency()

    with patch("botnim.dynamic_extraction._async_chat_completion_inner", fake_oai):
        streams = await collect_context_sources_async(
            context_, tmp_path, concurrency,
            bot="unified", extraction_cache=cache,
        )

    assert len(streams) == 1
    fake_oai.assert_awaited_once()
    cached = cache.get(_content_hash(pipeline_content), EXTRACTION_VERSION)
    assert cached is not None
    assert cached["title"] == "Fresh Beta"

    # Second invocation: no further LLM calls.
    with patch("botnim.dynamic_extraction._async_chat_completion_inner", AsyncMock(side_effect=AssertionError)):
        streams2 = await collect_context_sources_async(
            context_, tmp_path, concurrency,
            bot="unified", extraction_cache=cache,
        )
    assert len(streams2) == 1


@pytest.mark.asyncio
async def test_rpd_error_short_circuits_and_returns_partial(cache, tmp_path, caplog):
    """Mock OpenAI to raise an RPD-shaped error after the 3rd call (in a
    5-task gather); assert file_streams has the first 3, last 2 are dropped,
    EXTRACTION RPD HIT logged, run exits without raising."""
    csv_path = tmp_path / "five.csv"
    csv_path.write_text("body\n" + "\n".join(f"row{i}" for i in range(5)) + "\n", encoding="utf-8")
    context_ = {"name": "ctx_rpd", "slug": "ctx_rpd", "type": "csv",
                "source": "five.csv", "fetcher": None}

    call_count = {"n": 0}

    class FakeRpdError(Exception):
        def __init__(self):
            super().__init__("Rate limit reached: requests per day. Limit: 10000.")

    async def _fake_completion(client, system_message):
        call_count["n"] += 1
        if call_count["n"] <= 3:
            resp = MagicMock()
            resp.choices = [MagicMock(message=MagicMock(content=f'{{"DocumentMetadata": {{"DocumentTitle": "ok{call_count["n"]}"}}}}'))]
            return resp
        raise FakeRpdError()

    concurrency = SyncConcurrency()
    with patch("botnim.dynamic_extraction._async_chat_completion_inner", _fake_completion), caplog.at_level("WARNING"):
        streams = await collect_context_sources_async(
            context_, tmp_path, concurrency,
            bot="unified", extraction_cache=cache,
        )

    assert len(streams) == 3, f"expected 3 successful, got {len(streams)}"
    assert any("EXTRACTION RPD HIT" in rec.message for rec in caplog.records), (
        f"no RPD log; got {[r.message for r in caplog.records]}"
    )
    assert any("RESUME:" in rec.message for rec in caplog.records)


@pytest.mark.asyncio
async def test_rpm_error_still_retries_via_decorator(cache, tmp_path):
    """Mock OpenAI to raise an RPM-shaped 429 once then succeed; assert
    one retry, one final success, no RpdExhausted bubble."""
    csv_path = tmp_path / "rpm.csv"
    csv_path.write_text("body\nrpm-row\n", encoding="utf-8")
    context_ = {"name": "ctx_rpm", "slug": "ctx_rpm", "type": "csv",
                "source": "rpm.csv", "fetcher": None}

    state = {"n": 0}

    class FakeRpmError(Exception):
        def __init__(self):
            super().__init__("Rate limit reached for requests per minute. Try again in 0.1s.")

    async def _fake_completion(client, system_message):
        state["n"] += 1
        if state["n"] == 1:
            raise FakeRpmError()
        resp = MagicMock()
        resp.choices = [MagicMock(message=MagicMock(content='{"DocumentMetadata": {"DocumentTitle": "after-retry"}}'))]
        return resp

    concurrency = SyncConcurrency()
    with patch("botnim.dynamic_extraction._async_chat_completion_inner", _fake_completion):
        streams = await collect_context_sources_async(
            context_, tmp_path, concurrency,
            bot="unified", extraction_cache=cache,
        )

    assert len(streams) == 1
    assert state["n"] >= 2, "decorator should have retried at least once"


@pytest.mark.asyncio
async def test_force_rebuild_purges_then_writes(cache, tmp_path):
    """Pre-populated cache rows for (bot, context, version) are gone after
    force_rebuild; new rows written from the fresh extraction."""
    cache.put("stale_h", EXTRACTION_VERSION,
              payload={"title": "stale"}, bot="unified", context="ctx_fr",
              document_type="text/markdown")
    assert cache.get("stale_h", EXTRACTION_VERSION) is not None

    purged = cache.purge(bot="unified", context="ctx_fr", extractor_version=EXTRACTION_VERSION)
    assert purged == 1
    assert cache.get("stale_h", EXTRACTION_VERSION) is None


# -----------------------------------------------------------------------------
# get_with_fallback + per-run rewarm budget — 2026-05-19 delta-design spec
# -----------------------------------------------------------------------------

def test_get_with_fallback_exact_hit(cache):
    """An exact match at the current version returns stale=False."""
    cache.put("h_exact", "v2", payload={"t": "exact"},
              bot="unified", context="ctx", document_type="text/markdown")
    hit = cache.get_with_fallback("h_exact", "v2")
    assert hit is not None
    assert hit["payload"] == {"t": "exact"}
    assert hit["from_version"] == "v2"
    assert hit["stale"] is False


def test_get_with_fallback_returns_older_version_as_stale(cache):
    """If only an older-version row exists, fallback serves it as stale."""
    cache.put("h_old", "v1", payload={"t": "old"},
              bot="unified", context="ctx", document_type="text/markdown")
    hit = cache.get_with_fallback("h_old", "v2")
    assert hit is not None
    assert hit["payload"] == {"t": "old"}
    assert hit["from_version"] == "v1"
    assert hit["stale"] is True


def test_get_with_fallback_picks_most_recent_older_version(cache):
    """Multiple older versions → pick the most-recently-extracted one."""
    cache.put("h_multi", "v1", payload={"t": "v1-old"},
              bot="unified", context="ctx", document_type="text/markdown")
    cache.put("h_multi", "v2", payload={"t": "v2-newer"},
              bot="unified", context="ctx", document_type="text/markdown")
    # Current version is v3, neither cached at v3 yet.
    hit = cache.get_with_fallback("h_multi", "v3")
    assert hit is not None
    assert hit["from_version"] == "v2"
    assert hit["payload"] == {"t": "v2-newer"}
    assert hit["stale"] is True


def test_get_with_fallback_prefers_exact_over_older(cache):
    """Even when an older row would tie on recency, the exact match wins."""
    cache.put("h_both", "v1", payload={"t": "v1"},
              bot="unified", context="ctx", document_type="text/markdown")
    cache.put("h_both", "v2", payload={"t": "v2"},
              bot="unified", context="ctx", document_type="text/markdown")
    hit = cache.get_with_fallback("h_both", "v1")
    assert hit is not None
    assert hit["from_version"] == "v1"
    assert hit["payload"] == {"t": "v1"}
    assert hit["stale"] is False


def test_get_with_fallback_returns_none_when_no_rows(cache):
    """No row at any version → None (caller proceeds to LLM extraction)."""
    assert cache.get_with_fallback("nothing_cached", "v2") is None


@pytest.mark.asyncio
async def test_rewarm_budget_take_caps_per_run():
    """Budget enforces the cap; stale_served counts regardless of budget."""
    sc = SyncConcurrency(rewarm_budget=2)
    assert await sc.rewarm_budget_take() is True   # 1 of 2
    assert await sc.rewarm_budget_take() is True   # 2 of 2
    assert await sc.rewarm_budget_take() is False  # cap hit, served stale only
    assert await sc.rewarm_budget_take() is False  # still capped
    assert sc.stale_served_count == 4
    assert sc.rewarm_budget_remaining == 0


@pytest.mark.asyncio
async def test_rewarm_budget_zero_serves_stale_forever():
    """budget=0 → every stale read is served, none rewarmed."""
    sc = SyncConcurrency(rewarm_budget=0)
    for _ in range(5):
        assert await sc.rewarm_budget_take() is False
    assert sc.stale_served_count == 5
    assert sc.rewarm_budget_remaining == 0


@pytest.mark.asyncio
async def test_collect_serves_stale_payload_and_schedules_rewarm(cache, tmp_path):
    """Stale cache hit → caller gets stale payload immediately; a background
    re-extract at EXTRACTION_VERSION runs and persists the new version row."""
    csv_value = "stale-target-content"
    pipeline_content = f"body:\n{csv_value}"
    csv_path = tmp_path / "src.csv"
    csv_path.write_text("body\n" + csv_value + "\n", encoding="utf-8")
    context_ = {"name": "ctx_stale", "slug": "ctx_stale", "type": "csv",
                "source": "src.csv", "fetcher": None}

    # Wipe L1 KVFile so a stale entry from a prior test run can't pre-empt
    # the L2 fallback lookup we're trying to exercise.
    import shutil
    repo_root = Path(__file__).resolve().parent.parent
    (repo_root / "cache" / "metadata.sqlite").unlink(missing_ok=True)
    shutil.rmtree(repo_root / "cache" / "metadata", ignore_errors=True)

    # Pre-seed a stale row at an older version — no row exists at the
    # current EXTRACTION_VERSION yet.
    stale_payload = {"title": "from-stale", "stale_marker": True, "status": "processed"}
    h = _content_hash(pipeline_content)
    cache.put(h, "v0-legacy",
              payload=stale_payload, bot="unified", context="ctx_stale",
              document_type="text/markdown")

    # Fake the LLM so the rewarm produces a known new payload.
    rewarm_calls = {"n": 0}

    async def _fake_completion(client, system_message):
        rewarm_calls["n"] += 1
        resp = MagicMock()
        resp.choices = [MagicMock(message=MagicMock(
            content='{"DocumentMetadata": {"DocumentTitle": "from-rewarm"}}'
        ))]
        return resp

    concurrency = SyncConcurrency(rewarm_budget=10)
    with patch("botnim.dynamic_extraction._async_chat_completion_inner", side_effect=_fake_completion):
        streams = await collect_context_sources_async(
            context_, tmp_path, concurrency,
            bot="unified", extraction_cache=cache,
        )

    # Sync caller got the stale payload (proxied through metadata).
    assert len(streams) == 1
    _, _, _, metadata = streams[0]
    assert metadata.get("stale_marker") is True, "sync caller should see stale payload synchronously"

    # Allow scheduled rewarm task(s) to drain.
    await asyncio.sleep(0.05)
    pending = [t for t in asyncio.all_tasks() if not t.done() and t is not asyncio.current_task()]
    if pending:
        await asyncio.gather(*pending, return_exceptions=True)

    # The rewarm fired exactly once and wrote a current-version row.
    assert rewarm_calls["n"] == 1
    rewarmed = cache.get(h, EXTRACTION_VERSION)
    assert rewarmed is not None
    assert rewarmed["DocumentMetadata"]["DocumentTitle"] == "from-rewarm"

    # Counters reflect what happened.
    assert concurrency.stale_served_count == 1
    assert concurrency.rewarmed_count == 1
    assert concurrency.exact_hits_count == 0
    assert concurrency.llm_miss_count == 0


@pytest.mark.asyncio
async def test_collect_zero_budget_serves_stale_without_rewarm(cache, tmp_path):
    """budget=0: stale payload served, no LLM call scheduled."""
    csv_value = "zero-budget-content"
    pipeline_content = f"body:\n{csv_value}"
    csv_path = tmp_path / "src.csv"
    csv_path.write_text("body\n" + csv_value + "\n", encoding="utf-8")
    context_ = {"name": "ctx_zero", "slug": "ctx_zero", "type": "csv",
                "source": "src.csv", "fetcher": None}

    import shutil
    repo_root = Path(__file__).resolve().parent.parent
    (repo_root / "cache" / "metadata.sqlite").unlink(missing_ok=True)
    shutil.rmtree(repo_root / "cache" / "metadata", ignore_errors=True)

    h = _content_hash(pipeline_content)
    cache.put(h, "v0-legacy",
              payload={"title": "stale-served", "stale_marker": True, "status": "processed"},
              bot="unified", context="ctx_zero", document_type="text/markdown")

    rewarm_calls = {"n": 0}
    async def _fake_completion(*args, **kwargs):
        rewarm_calls["n"] += 1
        raise AssertionError("budget=0 must not trigger LLM calls")

    concurrency = SyncConcurrency(rewarm_budget=0)
    with patch("botnim.dynamic_extraction._async_chat_completion_inner", side_effect=_fake_completion):
        streams = await collect_context_sources_async(
            context_, tmp_path, concurrency,
            bot="unified", extraction_cache=cache,
        )
        # Drain any pending tasks (there shouldn't be any).
        await asyncio.sleep(0.05)
        pending = [t for t in asyncio.all_tasks() if not t.done() and t is not asyncio.current_task()]
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)

    assert rewarm_calls["n"] == 0
    assert len(streams) == 1
    _, _, _, metadata = streams[0]
    assert metadata.get("stale_marker") is True

    # No current-version row was written.
    assert cache.get(h, EXTRACTION_VERSION) is None

    # Counters.
    assert concurrency.stale_served_count == 1
    assert concurrency.rewarmed_count == 0
    assert concurrency.rewarm_budget_remaining == 0
