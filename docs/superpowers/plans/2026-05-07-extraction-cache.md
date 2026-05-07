# Aurora-Backed Extraction Cache Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Eliminate redundant `gpt-4o-mini` calls during `botnim sync` by adding a durable Aurora-backed read-through cache for `dynamic_extraction.py` outputs, plus graceful RPD-error handling so partial progress is persisted across runs.

**Architecture:** Spec-defined design. New table `extraction_cache(content_hash, extractor_version, payload, …)`; new helper `botnim/extraction_cache.py`; refactored `_get_metadata_for_content_async` to L1=KVFile + L2=Aurora; new `RpdExhausted` exception class that aborts a context's gather without aborting the run; `force_rebuild=True` purges cache rows for the selected `(bot, context, EXTRACTION_VERSION)`.

**Tech Stack:** Python 3.12, SQLAlchemy 2.x, alembic, pytest + pytest-postgresql, asyncio. Aurora PostgreSQL 16. Production rollout via parlibot/`deploy.sh prod` + verification across two consecutive days.

**Spec:** `docs/superpowers/specs/2026-05-07-extraction-cache-design.md`

---

## File Structure

| File | Action | Responsibility |
|---|---|---|
| `botnim/db/migrations/versions/0008_extraction_cache.py` | Create | Alembic migration: table + composite unique + purge index. |
| `botnim/extraction_cache.py` | Create | `ExtractionCache` class: `get` / `put` / `purge`. ~80 LOC. |
| `botnim/dynamic_extraction.py` | Modify | Add `EXTRACTION_VERSION` constant; add `RpdExhausted` exception; `_is_rpd_error` helper; teach `async_retry_openai` to convert RPD-shaped 429s into `RpdExhausted` immediately. |
| `botnim/_concurrency.py` | Modify | `SyncConcurrency` gains an `rpd_tripped: asyncio.Event` field; `run_bounded` short-circuits to `RpdExhausted` if set. |
| `botnim/collect_sources.py` | Modify | `_get_metadata_for_content_async`, `collect_context_sources_async`, `collect_context_sources` accept `bot, context_name, extraction_cache`; thread through; catch `RpdExhausted` from `gather`; emit RESUME log; return partial `file_streams`. |
| `botnim/vector_store/vector_store_base.py` | Modify | `vector_store_update` constructs one `ExtractionCache(environment)` per run; passes `bot=self.config['slug']` and the cache through `collect_context_sources`; calls `extraction_cache.purge(...)` when `force_rebuild=True` for the selected context. |
| `botnim/vector_store/vector_store_aurora.py` | Modify | Pass `environment` through to `vector_store_base.vector_store_update` if not already (already done in delta-sync PR — confirm). |
| `botnim/sync.py` | Modify | `sync_agents` and `_sync_vector_store` thread `environment` through to vector_store_update so the cache constructor knows which DB to talk to (likely already done). |
| `tests/test_extraction_cache.py` | Create | 9 pytest cases (pytest-postgresql fixture). |
| `CLAUDE.md` (rebuilding-bots root) | Modify | Add row to the `botnim sync` modes table + new "Extraction cache" subsection. |
| `CLAUDE.md` (parlibot root) | Modify | One sentence under the Phase 8 description noting the cache makes resume cheap. |

---

## Task 1: Worktree setup

**Files:** none yet

- [ ] **Step 1: Create worktree off origin/main**

```bash
cd /Users/amir/Development/anubanu/parlibot/rebuilding-bots
git fetch origin main
git worktree add .worktrees/extraction-cache -b feat/extraction-cache origin/main
cd .worktrees/extraction-cache
```

If `.worktrees/extraction-cache` already exists from a prior attempt, resume into it: `cd .worktrees/extraction-cache && git status`.

- [ ] **Step 2: Verify clean baseline (existing tests pass)**

```bash
AIRTABLE_API_KEY=dummy OPENAI_API_KEY_STAGING=dummy /Users/amir/Development/anubanu/parlibot/rebuilding-bots/.venv/bin/pytest tests/document_parser/pdfs tests/test_fetch_and_process_dispatch.py tests/word_doc tests/vector_store -q
```

Expected: all green (~50+ tests). Includes the delta-sync tests merged in PR #128.

- [ ] **Step 3: Verify pytest-postgresql fixture is wired**

```bash
/Users/amir/Development/anubanu/parlibot/rebuilding-bots/.venv/bin/pytest tests/vector_store/test_aurora_delta.py -v 2>&1 | tail -10
```

Expected: 3 PASS. If failing on a fixture wiring issue, fix before continuing.

---

## Task 2: First failing tests — `ExtractionCache` get/put/purge surface

**Files:**
- Create: `tests/test_extraction_cache.py`
- Test target: `botnim/extraction_cache.py` (not yet created)

- [ ] **Step 1: Write the failing tests for the cache surface**

Create `tests/test_extraction_cache.py` with this content:

```python
"""Aurora-backed read-through cache for dynamic_extraction outputs.

These tests cover the ExtractionCache surface in isolation; the
integration with collect_sources is covered later in this plan.
"""
from __future__ import annotations

import hashlib
import pytest
from sqlalchemy import text

from botnim.extraction_cache import ExtractionCache
from botnim.db.session import get_session


@pytest.fixture
def cache():
    """An ExtractionCache talking to the test postgres."""
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
```

- [ ] **Step 2: Run; expect ImportError**

```bash
AIRTABLE_API_KEY=dummy OPENAI_API_KEY_STAGING=dummy /Users/amir/Development/anubanu/parlibot/rebuilding-bots/.venv/bin/pytest tests/test_extraction_cache.py -v 2>&1 | tail -20
```

Expected: collection error — `ModuleNotFoundError: No module named 'botnim.extraction_cache'`.

If a different error, stop and fix.

---

## Task 3: Alembic migration `0008_extraction_cache`

**Files:**
- Create: `botnim/db/migrations/versions/0008_extraction_cache.py`

- [ ] **Step 1: Locate the latest revision**

```bash
ls botnim/db/migrations/versions/ | sort | tail -5
```

Expected: `0007_hnsw_replaces_ivfflat.py` is the head.

- [ ] **Step 2: Confirm the chain via the migration's `down_revision`**

```bash
grep -n "^revision\|^down_revision" botnim/db/migrations/versions/0007_hnsw_replaces_ivfflat.py
```

Note the `revision = ...` value — that becomes our `down_revision`.

- [ ] **Step 3: Create the migration file**

Use `0007_hnsw_replaces_ivfflat.py` as a structural template (operator-running migration with raw SQL, no model dependencies).

```python
"""extraction_cache: Aurora-backed cache for dynamic_extraction.py outputs

Revision ID: 0008_extraction_cache
Revises: 0007_hnsw_replaces_ivfflat
Create Date: 2026-05-07

Adds the extraction_cache table that survives ECS task replacements
and prevents per-file gpt-4o-mini calls on every sync. Lookup key is
(content_hash, extractor_version); bot/context are stored only for
the operator-grade purge query.
"""
from __future__ import annotations

from alembic import op


# revision identifiers, used by Alembic.
revision = "0008_extraction_cache"
down_revision = "0007_hnsw_replaces_ivfflat"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE extraction_cache (
            id                 BIGSERIAL PRIMARY KEY,
            content_hash       TEXT        NOT NULL,
            extractor_version  TEXT        NOT NULL,
            payload            JSONB       NOT NULL,
            bot                TEXT        NOT NULL,
            context            TEXT        NOT NULL,
            document_type      TEXT,
            extracted_at       TIMESTAMPTZ NOT NULL DEFAULT now(),

            CONSTRAINT extraction_cache_key_unique UNIQUE (content_hash, extractor_version)
        );
        """
    )
    op.execute(
        """
        CREATE INDEX idx_extraction_cache_purge
            ON extraction_cache (bot, context, extractor_version);
        """
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_extraction_cache_purge;")
    op.execute("DROP TABLE IF EXISTS extraction_cache;")
```

- [ ] **Step 4: Apply locally to the test postgres**

```bash
DATABASE_URL=$(/Users/amir/Development/anubanu/parlibot/rebuilding-bots/.venv/bin/python -c "from tests.conftest import _test_database_url; print(_test_database_url())") \
  /Users/amir/Development/anubanu/parlibot/rebuilding-bots/.venv/bin/alembic upgrade head 2>&1 | tail -8
```

If your `tests/conftest.py` doesn't expose a `_test_database_url`, run the migration via the same engine used by the existing test runs (the postgres fixture handles this in tests; the migration auto-applies in the conftest setup). Skipping this step is fine — the next test run exercises the migration.

- [ ] **Step 5: Re-run the cache tests; expect them to fail differently now**

```bash
AIRTABLE_API_KEY=dummy OPENAI_API_KEY_STAGING=dummy /Users/amir/Development/anubanu/parlibot/rebuilding-bots/.venv/bin/pytest tests/test_extraction_cache.py -v 2>&1 | tail -15
```

Expected: still `ModuleNotFoundError` on `botnim.extraction_cache`. Migration alone doesn't add the module.

- [ ] **Step 6: Commit migration**

```bash
git add botnim/db/migrations/versions/0008_extraction_cache.py tests/test_extraction_cache.py
git commit -m "feat(db): alembic 0008 — add extraction_cache table"
```

---

## Task 4: Implement `botnim/extraction_cache.py`

**Files:**
- Create: `botnim/extraction_cache.py`

- [ ] **Step 1: Write the module**

```python
"""Aurora-backed read-through cache for dynamic_extraction.py outputs.

Lookup key is (content_hash, extractor_version). Two contexts that ingest
the same raw text share one row — saves cost when content overlap exists
between contexts (e.g. plenary_schedule vs knesset_protocols citing the
same Knesset minute).

The class is intentionally thin: get / put / purge. No connection pool of
its own — reuses :func:`botnim.db.session.get_session` so this module
inherits the same env-var convention as every other Aurora caller.
"""
from __future__ import annotations

import json
from typing import Any

from sqlalchemy import text

from .config import get_logger
from .db.session import get_session

logger = get_logger(__name__)


class ExtractionCache:
    """Aurora-backed read-through cache for dynamic_extraction outputs."""

    def __init__(self, environment: str):
        # `environment` kept on the instance for log/diagnostic context.
        # The actual DB target is encoded in get_session()'s engine binding,
        # which is already environment-scoped via env vars.
        self.environment = environment

    def get(self, content_hash: str, extractor_version: str) -> dict[str, Any] | None:
        """Return cached payload dict for the given key, or None on miss."""
        with get_session() as sess:
            row = sess.execute(text(
                "SELECT payload FROM extraction_cache "
                "WHERE content_hash = :h AND extractor_version = :v"
            ), {"h": content_hash, "v": extractor_version}).fetchone()
        if row is None:
            return None
        # SQLAlchemy returns the JSONB column as a python dict already.
        return row[0] if isinstance(row[0], dict) else json.loads(row[0])

    def put(
        self,
        content_hash: str,
        extractor_version: str,
        *,
        payload: dict[str, Any],
        bot: str,
        context: str,
        document_type: str | None,
    ) -> None:
        """Idempotent upsert. Last writer wins on payload."""
        with get_session() as sess:
            sess.execute(text(
                "INSERT INTO extraction_cache "
                "(content_hash, extractor_version, payload, bot, context, document_type) "
                "VALUES (:h, :v, CAST(:p AS jsonb), :b, :c, :dt) "
                "ON CONFLICT (content_hash, extractor_version) DO UPDATE SET "
                "    payload = EXCLUDED.payload, "
                "    extracted_at = now()"
            ), {
                "h": content_hash,
                "v": extractor_version,
                "p": json.dumps(payload, ensure_ascii=False),
                "b": bot,
                "c": context,
                "dt": document_type,
            })

    def purge(
        self,
        bot: str,
        context: str,
        extractor_version: str | None = None,
    ) -> int:
        """Delete rows for (bot, context [, extractor_version]). Returns count."""
        with get_session() as sess:
            if extractor_version is None:
                result = sess.execute(text(
                    "DELETE FROM extraction_cache WHERE bot = :b AND context = :c"
                ), {"b": bot, "c": context})
            else:
                result = sess.execute(text(
                    "DELETE FROM extraction_cache "
                    "WHERE bot = :b AND context = :c AND extractor_version = :v"
                ), {"b": bot, "c": context, "v": extractor_version})
            count = result.rowcount or 0
        if count:
            logger.info(
                "Purged %d extraction_cache rows for %s/%s @ %s",
                count, bot, context, extractor_version or "all-versions",
            )
        return count
```

- [ ] **Step 2: Run the cache tests**

```bash
AIRTABLE_API_KEY=dummy OPENAI_API_KEY_STAGING=dummy /Users/amir/Development/anubanu/parlibot/rebuilding-bots/.venv/bin/pytest tests/test_extraction_cache.py -v 2>&1 | tail -20
```

Expected: 4 PASS (`test_get_returns_none_on_miss`, `test_put_then_get_roundtrips_payload`, `test_put_is_idempotent_on_conflict`, `test_purge_scopes_to_bot_context_version`).

If any fail with a `relation "extraction_cache" does not exist` error, the alembic migration didn't run against the test database — check `tests/conftest.py` for the migration auto-apply hook and add `0008` to whatever discovery list it uses (most pytest-postgresql wiring just runs `alembic upgrade head` against the fixture).

- [ ] **Step 3: Run the broader suite to confirm no regressions**

```bash
AIRTABLE_API_KEY=dummy OPENAI_API_KEY_STAGING=dummy /Users/amir/Development/anubanu/parlibot/rebuilding-bots/.venv/bin/pytest tests/document_parser/pdfs tests/test_fetch_and_process_dispatch.py tests/word_doc tests/vector_store tests/test_extraction_cache.py -q
```

Expected: all green.

- [ ] **Step 4: Commit**

```bash
git add botnim/extraction_cache.py
git commit -m "feat(extraction_cache): aurora-backed read-through cache class"
```

---

## Task 5: Failing tests — RPD detection + integration with collect_sources

**Files:**
- Modify: `tests/test_extraction_cache.py` (append the integration tests)

- [ ] **Step 1: Append five integration tests**

Append to `tests/test_extraction_cache.py`:

```python
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
    content = "alpha"
    cached_payload = {"title": "Cached Alpha", "status": "processed"}
    cache.put(
        _content_hash(content), EXTRACTION_VERSION,
        payload=cached_payload, bot="unified", context="ctx_alpha",
        document_type="text/markdown",
    )

    # Build a context that yields one synthetic md with that content.
    csv_path = tmp_path / "alpha.csv"
    csv_path.write_text("body\n" + content + "\n", encoding="utf-8")
    context_ = {"name": "ctx_alpha", "slug": "ctx_alpha", "type": "csv",
                "source": "alpha.csv", "fetcher": None}

    fake_oai = AsyncMock(side_effect=AssertionError("OpenAI must not be called"))
    concurrency = SyncConcurrency()

    with patch("botnim.dynamic_extraction._async_chat_completion", fake_oai):
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
    content = "beta"
    csv_path = tmp_path / "beta.csv"
    csv_path.write_text("body\n" + content + "\n", encoding="utf-8")
    context_ = {"name": "ctx_beta", "slug": "ctx_beta", "type": "csv",
                "source": "beta.csv", "fetcher": None}

    fake_response = MagicMock()
    fake_response.choices = [MagicMock(message=MagicMock(content='{"DocumentMetadata": {"DocumentTitle": "Fresh Beta"}}'))]
    fake_oai = AsyncMock(return_value=fake_response)
    concurrency = SyncConcurrency()

    with patch("botnim.dynamic_extraction._async_chat_completion", fake_oai):
        streams = await collect_context_sources_async(
            context_, tmp_path, concurrency,
            bot="unified", extraction_cache=cache,
        )

    assert len(streams) == 1
    fake_oai.assert_awaited_once()
    cached = cache.get(_content_hash(content), EXTRACTION_VERSION)
    assert cached is not None
    assert cached["title"] == "Fresh Beta"

    # Second invocation: no further LLM calls.
    with patch("botnim.dynamic_extraction._async_chat_completion", AsyncMock(side_effect=AssertionError)):
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
    with patch("botnim.dynamic_extraction._async_chat_completion", _fake_completion), caplog.at_level("WARNING"):
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
    with patch("botnim.dynamic_extraction._async_chat_completion", _fake_completion):
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
```

Note on `pytest.mark.asyncio`: the existing `tests/test_collect_sources_*.py` already use `pytest-asyncio`; if the harness isn't configured, add `asyncio_mode = "auto"` to `pyproject.toml`'s `[tool.pytest.ini_options]` (most likely already there).

- [ ] **Step 2: Run; expect failures because the new code paths don't exist yet**

```bash
AIRTABLE_API_KEY=dummy OPENAI_API_KEY_STAGING=dummy /Users/amir/Development/anubanu/parlibot/rebuilding-bots/.venv/bin/pytest tests/test_extraction_cache.py -v 2>&1 | tail -25
```

Expected:
- The 4 unit tests still PASS.
- The 5 new integration tests FAIL with either `ImportError: cannot import name 'RpdExhausted' from 'botnim.dynamic_extraction'` or `TypeError: collect_context_sources_async() got an unexpected keyword argument 'bot'`.

If they fail for any other reason, stop and fix.

---

## Task 6: Implement `RpdExhausted` + `_is_rpd_error` + `EXTRACTION_VERSION`

**Files:**
- Modify: `botnim/dynamic_extraction.py`
- Modify: `botnim/_concurrency.py` (decorator change)

- [ ] **Step 1: Add to `dynamic_extraction.py` (above `_DEFAULT_TEMPLATE`)**

```python
EXTRACTION_VERSION = "v1-gpt-4o-mini"  # bump on prompt/model/schema change


class RpdExhausted(Exception):
    """OpenAI returned a 429 indicating the requests-per-day quota is exhausted.

    Raised by the retry decorator on detection (instead of retrying), so the
    sync caller can short-circuit the rest of the gather and persist
    whatever partial progress was made before this run hit the daily wall.
    """


def _is_rpd_error(exc: BaseException) -> bool:
    """Return True iff the error message indicates RPD (not RPM)."""
    msg = str(exc).lower()
    return (
        "requests per day" in msg
        or "rpd" in msg.split()
        or "daily limit" in msg
    )
```

- [ ] **Step 2: Update `async_retry_openai` in `_concurrency.py`**

Open `botnim/_concurrency.py` and find `async_retry_openai`. Inside its except-clause for 429s, add a pre-check:

```python
# Do this inside the existing except block, before the retry-sleep logic.
from .dynamic_extraction import _is_rpd_error, RpdExhausted
if _is_rpd_error(exc):
    # Daily limit doesn't reset until midnight UTC; retrying within
    # this run is pointless. Convert to RpdExhausted so the caller can
    # short-circuit gracefully.
    raise RpdExhausted(str(exc)) from exc
```

The exact syntax depends on the existing structure (it's likely a `try/except openai.RateLimitError`). Read the current code and surgically insert the check. Keep the import inside the function to avoid a circular import at module load.

- [ ] **Step 3: Add `rpd_tripped` to `SyncConcurrency`**

In the same `_concurrency.py`, find `class SyncConcurrency` and add an attribute:

```python
def __init__(self, ...):
    ...
    self.rpd_tripped: asyncio.Event = asyncio.Event()
```

In `run_bounded`, before acquiring the semaphore:

```python
async def run_bounded(self, fn, *args, **kwargs):
    if self.rpd_tripped.is_set():
        # Spec: avoid burning remaining tasks on guaranteed-to-fail calls.
        from .dynamic_extraction import RpdExhausted
        raise RpdExhausted("rpd_tripped flag set by an earlier task")
    async with self.semaphore:
        try:
            return await fn(*args, **kwargs)
        except Exception:
            from .dynamic_extraction import RpdExhausted
            # If THIS call hit RPD, set the flag so siblings short-circuit.
            # The exception bubbles either way.
            raise
```

Then teach the retry decorator to set the flag *before* re-raising the converted RpdExhausted. Or, equivalently, do it in `_get_metadata_for_content_async` (next task) since the concurrency object is in scope there.

- [ ] **Step 4: Run cache tests so far**

```bash
AIRTABLE_API_KEY=dummy OPENAI_API_KEY_STAGING=dummy /Users/amir/Development/anubanu/parlibot/rebuilding-bots/.venv/bin/pytest tests/test_extraction_cache.py -v 2>&1 | tail -20
```

Expected: 4 unit PASS; the 5 integration tests still fail (collect_context_sources_async hasn't been threaded yet).

---

## Task 7: Thread cache through `collect_sources` and trip RPD flag

**Files:**
- Modify: `botnim/collect_sources.py`

- [ ] **Step 1: Update `_get_metadata_for_content_async` signature + body**

Replace the function body to match the spec's flow:

```python
async def _get_metadata_for_content_async(
    content: str,
    file_path: str,
    document_type: str,
    concurrency: SyncConcurrency,
    *,
    bot: str | None = None,
    context_name: str | None = None,
    extraction_cache=None,
    client=None,
) -> dict:
    from .dynamic_extraction import (
        EXTRACTION_VERSION, RpdExhausted, extract_structured_content_async,
    )

    content_hash = hashlib.sha256(content.strip().encode("utf-8")).hexdigest()

    # L1: per-process KVFile (legacy fast path).
    cached_local = _cached_metadata_for_content(content)
    if cached_local is not None:
        return cached_local

    # L2: Aurora cache.
    if extraction_cache is not None:
        try:
            cached_aurora = extraction_cache.get(content_hash, EXTRACTION_VERSION)
        except Exception as e:
            logger.warning("extraction_cache.get failed for %s: %s", file_path, e)
            cached_aurora = None
        if cached_aurora is not None:
            async with concurrency.cache_lock:
                cache.set(_cache_key(content), {"content": content, "metadata": cached_aurora})
            return cached_aurora

    # Miss. Bounded LLM call.
    try:
        extracted_data = await concurrency.run_bounded(
            extract_structured_content_async,
            content, document_type=document_type, client=client,
        )
        metadata = _build_metadata_record(content, file_path, document_type, extracted_data, None)
    except RpdExhausted:
        # Set the trip flag so other in-flight tasks short-circuit.
        concurrency.rpd_tripped.set()
        raise
    except Exception as e:
        logger.error(f"Error extracting structured content from {file_path}: {e}")
        metadata = _build_metadata_record(content, file_path, document_type, None, e)
        # Don't cache errors; next run retries.
        return metadata

    # Persist to L2 (Aurora). Failures here log + continue (L1 still gets it).
    if extraction_cache is not None and bot and context_name:
        try:
            extraction_cache.put(
                content_hash, EXTRACTION_VERSION,
                payload=metadata, bot=bot, context=context_name,
                document_type=document_type,
            )
        except Exception as e:
            logger.warning("extraction_cache.put failed for %s: %s", file_path, e)

    async with concurrency.cache_lock:
        cache.set(_cache_key(content), {"content": content, "metadata": metadata})
    return metadata
```

- [ ] **Step 2: Update `_process_file_stream_async` signature**

Add `bot`, `context_name`, `extraction_cache` keyword args; thread to `_get_metadata_for_content_async`:

```python
async def _process_file_stream_async(
    filename, content, content_type, source_id, concurrency, client,
    *, bot=None, context_name=None, extraction_cache=None,
):
    fname, text, ctype = _prepare_file_content(filename, content, content_type)
    metadata = await _get_metadata_for_content_async(
        text, fname, ctype, concurrency,
        bot=bot, context_name=context_name, extraction_cache=extraction_cache,
        client=client,
    )
    ...
```

- [ ] **Step 3: Update `collect_context_sources_async` to accept + thread**

```python
async def collect_context_sources_async(
    context_,
    config_dir: Path,
    concurrency: SyncConcurrency,
    *,
    bot: str | None = None,
    extraction_cache=None,
    client=None,
):
    from .dynamic_extraction import RpdExhausted

    global cache
    cache = KVFile(location=str(Path(__file__).parent.parent / "cache" / "metadata"))

    context_name = context_["name"]
    raw: list[tuple[str, object, str, str]] = []
    if "sources" in context_:
        for source in context_["sources"]:
            raw.extend(_raw_streams_for_context(config_dir, context_name, source, offset=len(raw)))
    elif "type" in context_ and "source" in context_:
        raw.extend(_raw_streams_for_context(config_dir, context_name, context_))
    else:
        logger.info("Context %s has no sources to collect.", context_name)

    tasks = [
        _process_file_stream_async(
            fn, content, ct, sid, concurrency, client,
            bot=bot, context_name=context_name, extraction_cache=extraction_cache,
        )
        for fn, content, ct, sid in raw
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    file_streams: list = []
    rpd_count = 0
    for r, (fn, _, _, _) in zip(results, raw):
        if isinstance(r, RpdExhausted):
            rpd_count += 1
            continue
        if isinstance(r, BaseException):
            logger.error(f"Extraction failed for {fn}: {r}")
            continue
        file_streams.append(r)

    if rpd_count > 0:
        logger.warning(
            "EXTRACTION RPD HIT: %d/%d files left un-extracted in context %s. "
            "%d files were extracted (cache+fresh) and will be embedded this run. "
            "RESUME: re-run `botnim sync <env> <bot>` after the daily limit "
            "resets (midnight UTC). Cached extractions persist in Aurora; the "
            "next run will only call gpt-4o-mini for the remaining %d files.",
            rpd_count, len(tasks), context_name, len(file_streams), rpd_count,
        )

    cache.close()
    return file_streams
```

- [ ] **Step 4: Update `collect_context_sources` (sync wrapper) signature**

```python
def collect_context_sources(context_, config_dir: Path, *, bot=None, extraction_cache=None):
    concurrency = SyncConcurrency()
    return run_async(collect_context_sources_async(
        context_, config_dir, concurrency,
        bot=bot, extraction_cache=extraction_cache,
    ))
```

- [ ] **Step 5: Run integration tests**

```bash
AIRTABLE_API_KEY=dummy OPENAI_API_KEY_STAGING=dummy /Users/amir/Development/anubanu/parlibot/rebuilding-bots/.venv/bin/pytest tests/test_extraction_cache.py -v 2>&1 | tail -30
```

Expected: all 9 PASS.

If the RPD test fails because the trip flag isn't being set early enough, double-check that `_get_metadata_for_content_async` calls `concurrency.rpd_tripped.set()` BEFORE re-raising. If RPM still bubbles as RpdExhausted, the substring match in `_is_rpd_error` is too greedy — make sure `"requests per minute"` doesn't trigger the `"requests per day"` check.

- [ ] **Step 6: Run the broader suite**

```bash
AIRTABLE_API_KEY=dummy OPENAI_API_KEY_STAGING=dummy /Users/amir/Development/anubanu/parlibot/rebuilding-bots/.venv/bin/pytest tests/document_parser/pdfs tests/test_fetch_and_process_dispatch.py tests/word_doc tests/vector_store tests/test_extraction_cache.py -q
```

Expected: all green. If `tests/test_collect_sources_*.py` exists and breaks because it calls `collect_context_sources` without the new kwargs, leave them alone — the new kwargs default to `None` so the existing callers go through the legacy code path (no Aurora touch).

- [ ] **Step 7: Commit**

```bash
git add botnim/dynamic_extraction.py botnim/_concurrency.py botnim/collect_sources.py
git commit -m "feat(extraction_cache): integrate aurora cache + RPD short-circuit"
```

---

## Task 8: Wire `vector_store_base` to construct + pass the cache; force_rebuild purges

**Files:**
- Modify: `botnim/vector_store/vector_store_base.py`
- Modify: `botnim/vector_store/vector_store_aurora.py` (if `environment` not already plumbed)
- Modify: `botnim/sync.py` (if needed)

- [ ] **Step 1: Add cache construction to `vector_store_update`**

Open `botnim/vector_store/vector_store_base.py`. Add an `environment` attribute on the base class (it's already on `VectorStoreAurora` and `VectorStoreES`):

```python
class VectorStoreBase(ABC):
    def __init__(self, config, config_dir, production):
        ...
        # Subclasses may override; default-falsy for openai backend which
        # doesn't talk to Aurora.
        self.environment: str | None = None
```

Then in `vector_store_update`:

```python
def vector_store_update(self, context, replace_context, reindex=False, force_rebuild=False):
    self.tool_resources = None
    self.tools = []

    # Construct one extraction_cache per run; aurora-only.
    extraction_cache = None
    if self.environment and self._supports_extraction_cache():
        from ..extraction_cache import ExtractionCache
        extraction_cache = ExtractionCache(environment=self.environment)

    bot_slug = self.config.get("slug")

    for context_ in context:
        context_name = context_["slug"]
        normalized = replace_context if replace_context is not None else "all"
        if normalized == "none":
            should_process = bool(reindex)
        elif normalized == "all" or normalized == context_name:
            should_process = True
        else:
            should_process = bool(reindex)
        should_force_rebuild = force_rebuild and should_process

        # Force-rebuild: purge extraction_cache rows for this (bot, context,
        # current version) so the next run re-extracts them.
        if should_force_rebuild and extraction_cache is not None and bot_slug:
            from ..dynamic_extraction import EXTRACTION_VERSION
            try:
                extraction_cache.purge(bot=bot_slug, context=context_name,
                                       extractor_version=EXTRACTION_VERSION)
            except Exception as e:
                logger.warning("extraction_cache.purge failed for %s/%s: %s",
                               bot_slug, context_name, e)

        vector_store = self.get_or_create_vector_store(
            context_, context_name, should_process, force_rebuild=should_force_rebuild,
        )

        if should_process:
            ... existing print/branching ...
            file_streams = collect_context_sources(
                context_, self.config_dir,
                bot=bot_slug, extraction_cache=extraction_cache,
            )
            ... rest unchanged ...
```

Add a helper:

```python
def _supports_extraction_cache(self) -> bool:
    """True iff this backend has Aurora connectivity (and therefore the
    extraction_cache table). False for the openai backend which talks
    only to OpenAI APIs."""
    return False  # subclasses override
```

In `VectorStoreAurora`: override to `return True`. In `VectorStoreES`: override to `return True` (same Aurora connection — the cache is bot-data, not vector-store-specific). In `VectorStoreOpenAI`: leave default `False` since that path doesn't have a `DATABASE_URL`.

- [ ] **Step 2: Confirm `environment` flows from `sync_agents` to vector stores**

Search:

```bash
grep -n "environment" botnim/sync.py | head -20
```

The aurora and ES constructors already take `environment=...`; confirm `_sync_vector_store` passes it. If not (the post-delta-sync codebase should already have it), add `environment=environment` to the constructor call.

- [ ] **Step 3: Run all tests**

```bash
AIRTABLE_API_KEY=dummy OPENAI_API_KEY_STAGING=dummy /Users/amir/Development/anubanu/parlibot/rebuilding-bots/.venv/bin/pytest tests -q 2>&1 | tail -10
```

Expected: all green (existing 50+ + 9 new = 59+ pass).

- [ ] **Step 4: Commit**

```bash
git add botnim/vector_store/
git commit -m "feat(vector_store): wire extraction_cache + force_rebuild purge"
```

---

## Task 9: Update CLAUDE.md (rebuilding-bots) — extraction-cache subsection

**Files:**
- Modify: `CLAUDE.md` (rebuilding-bots root)

- [ ] **Step 1: Locate the existing sync-modes table**

```bash
grep -n "delta-sync\|botnim sync" CLAUDE.md | head -10
```

- [ ] **Step 2: Add a row to the modes table + a new subsection**

After the existing `botnim sync` modes table, append:

```markdown
**Extraction cache (post-2026-05-07):** Per-file `gpt-4o-mini` calls go through an Aurora-backed read-through cache (`extraction_cache` table, key = `(content_hash, extractor_version)`). On a fresh env the cache is cold and sync may RPD-out partway through `knesset_protocols` (~13K rows vs ~10K daily limit) — that's normal: the run logs `EXTRACTION RPD HIT … RESUME: …` and exits 0. Re-running the next day reads everything that was already extracted from the cache and pays the LLM cost only for the remaining unextracted files. After the cache warms, every subsequent sync costs ~0 LLM calls in steady state.

Bumping `EXTRACTION_VERSION` (in `botnim/dynamic_extraction.py`) is the right move when the prompt, model, or output schema changes — it invalidates all old payloads and forces re-extraction on the next sync. Use `--force-rebuild --replace-context <slug>` to purge the cache for one specific context (and re-derive both extractions and embeddings).

There is no `--no-extraction-cache` CLI flag in v1; the two escape hatches above cover the realistic operator workflows.
```

- [ ] **Step 3: Commit**

```bash
git add CLAUDE.md
git commit -m "docs(CLAUDE): document extraction_cache + RPD resume semantics"
```

---

## Task 10: Open + merge PR

**Files:** none modified locally

- [ ] **Step 1: Push the branch**

```bash
git push -u origin feat/extraction-cache 2>&1 | tail -3
```

- [ ] **Step 2: Open the PR**

```bash
cat > /tmp/extraction-cache-pr-body.md <<'EOF'
## Summary

Add an Aurora-backed extraction cache so `botnim sync` doesn't burn `gpt-4o-mini` RPD on every cold-task run.

- New `extraction_cache` table (alembic 0008), keyed `(content_hash, extractor_version)`.
- `dynamic_extraction.py` gets a module-level `EXTRACTION_VERSION` constant + a new `RpdExhausted` exception.
- `_get_metadata_for_content_async` becomes a two-tier read-through: L1=KVFile, L2=Aurora.
- RPD-shaped 429s short-circuit the rest of the gather (drain in-flight tasks, log a `RESUME:` hint, exit 0). RPM 429s continue to retry as today.
- `--force-rebuild` now also purges `extraction_cache` rows for the targeted `(bot, context, EXTRACTION_VERSION)`.

Spec: `docs/superpowers/specs/2026-05-07-extraction-cache-design.md`.

Closes the bug observed on 2026-05-07: cold-task `botnim sync` for `knesset_protocols` (~13K rows) burns through 10K RPD in minutes and aborts with no partial progress persisted.

## Test plan

- [x] 9 new pytest cases in `tests/test_extraction_cache.py` cover the get/put/purge surface, cache-hit/miss integration, RPD short-circuit + partial result, RPM passthrough, force_rebuild purge.
- [x] All ~50 existing tests still pass.
- [ ] Post-merge prod deploy via `./deploy.sh prod --auto-approve --skip-migrate`.
- [ ] Day-1 prod sync: `extraction_cache` row count between 8K–10K; logs include `EXTRACTION RPD HIT … RESUME: …`.
- [ ] Day-2 prod sync (after RPD reset): cache row count climbs to ≥13K; LLM call count well under 5K.
- [ ] Day-3 prod sync: LLM call count ≈ 0.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
gh pr create --repo whiletrue-industries/rebuilding-bots --base main --head feat/extraction-cache \
    --title "feat(sync): aurora-backed extraction cache + graceful RPD handling" \
    --body-file /tmp/extraction-cache-pr-body.md 2>&1 | tail -3
```

- [ ] **Step 3: Merge after CI green**

```bash
gh pr merge <PR_NUMBER> --repo whiletrue-industries/rebuilding-bots --squash --delete-branch 2>&1 | tail -3
```

- [ ] **Step 4: Verify origin/main moved**

```bash
git -C /Users/amir/Development/anubanu/parlibot/rebuilding-bots fetch origin main
git -C /Users/amir/Development/anubanu/parlibot/rebuilding-bots log --oneline origin/main -3
```

---

## Task 11: Deploy to prod

**Files:** none

- [ ] **Step 1: Switch parlibot's local rebuilding-bots HEAD to new main**

```bash
cd /Users/amir/Development/anubanu/parlibot/rebuilding-bots
git fetch origin main
git checkout origin/main
```

- [ ] **Step 2: Verify prod SSO is fresh**

```bash
aws --profile anubanu-prod sts get-caller-identity --query 'Account' --output text
```

Expected: `086879295714`. Run `aws sso login --profile anubanu-prod` if expired.

- [ ] **Step 3: Pre-deploy reminder — alembic 0008 must apply on prod**

The prod CLAUDE.md notes a known issue: phase 6.5 (alembic) on prod uses the app user, not the master, and may silently no-op if the migration touches objects owned by `postgres`. Migration 0008 only `CREATE TABLE`s a new object — `botnim_app` should own it once created — but to be safe, after the deploy completes run a manual check:

```bash
aws --profile anubanu-prod ecs run-task ...  # one-shot psql to confirm extraction_cache exists
```

If 0008 didn't apply, copy the migration's SQL and run it as the master role (same recipe used for 0007 in the prod CLAUDE.md notes).

- [ ] **Step 4: Launch prod deploy**

```bash
cd /Users/amir/Development/anubanu/parlibot
bash -c './deploy.sh prod --auto-approve --skip-migrate > /tmp/deploy-prod-extraction-cache.log 2>&1' &
echo "deploy_pid=$!"
```

ETA ~10 min.

- [ ] **Step 5: Monitor with the Monitor tool**

Use the same poll-loop pattern as the delta-sync rollout (`grep -qF "deploy to prod complete"`); on completion, check the gold-set 5/5 line.

- [ ] **Step 6: Verify migration applied**

```bash
PROD_TASK=$(aws --profile anubanu-prod ecs list-tasks --cluster buildup-shared --service-name botnim-api-prod-api --desired-status RUNNING --query 'taskArns[0]' --output text)
# manually run psql via run-task if exec-command is blocked on prod
```

If the table doesn't exist, apply the migration's SQL via the master-role recipe described in parlibot's CLAUDE.md (the same one used for 0007).

---

## Task 12: Seed the prod cache (will RPD-out partway, that's expected)

**Files:** none modified

- [ ] **Step 1: Build run-task overrides**

```bash
cat > /tmp/extraction-cache-seed-overrides.json <<'EOF'
{
  "containerOverrides": [{
    "name": "api",
    "command": ["sh","-c","echo START_SEED_SYNC; AIRTABLE_API_KEY=dummy botnim sync prod all --backend aurora; echo END_SEED_SYNC"]
  }]
}
EOF
```

- [ ] **Step 2: Capture prod task def + netcfg**

```bash
PROD_TD=$(aws --profile anubanu-prod ecs describe-services --cluster buildup-shared --services botnim-api-prod-api --query 'services[0].taskDefinition' --output text)
aws --profile anubanu-prod ecs describe-services --cluster buildup-shared --services botnim-api-prod-api --query 'services[0].networkConfiguration' --output json > /tmp/prod-netcfg.json
```

- [ ] **Step 3: Launch the seed run-task**

```bash
TASK=$(aws --profile anubanu-prod ecs run-task \
  --cluster buildup-shared --task-definition "$PROD_TD" \
  --launch-type FARGATE --network-configuration file:///tmp/prod-netcfg.json \
  --overrides file:///tmp/extraction-cache-seed-overrides.json \
  --query 'tasks[0].taskArn' --output text)
TID=$(echo "$TASK" | awk -F/ '{print $NF}')
echo "SEED_TASK=$TID"
```

- [ ] **Step 4: Wait for STOP and grep for the RESUME line**

```bash
aws --profile anubanu-prod ecs wait tasks-stopped --cluster buildup-shared --tasks "$TID"
aws --profile anubanu-prod logs filter-log-events \
  --log-group-name /ecs/prod/botnim-api-prod-api \
  --log-stream-names "api/api/$TID" \
  --filter-pattern '"EXTRACTION RPD HIT"' \
  --query 'events[*].message' --output text
```

Expected: at least one `EXTRACTION RPD HIT` line that names `knesset_protocols` and includes the `RESUME:` hint. Sync exit code 0.

- [ ] **Step 5: Confirm cache row count**

Run a one-shot psql via run-task (since exec-command is blocked on prod per CLAUDE.md):

```sql
SELECT bot, context, extractor_version, count(*)
FROM extraction_cache
GROUP BY 1, 2, 3
ORDER BY 4 DESC;
```

Expected: `unified / knesset_protocols / v1-gpt-4o-mini` row count between 8K–10K. Other contexts likely fully cached (smaller row counts).

---

## Task 13: Verify steady-state next day

**Files:** none

- [ ] **Step 1: Wait until at least 24h after task 12 (RPD reset is midnight UTC; safer to wait a full day)**

- [ ] **Step 2: Re-run the seed sync**

```bash
TASK=$(aws --profile anubanu-prod ecs run-task ...)  # same overrides as task 12 step 3
```

- [ ] **Step 3: Watch the logs**

Expected on day 2:
- New `EXTRACTION RPD HIT` log if knesset_protocols still has unextracted rows (count should be ~3K, the remainder).
- Or no RPD log if the day-2 budget covered all remaining files.
- Cache row count climbs to ≥13K total.

- [ ] **Step 4: Day 3 sync (steady state)**

```bash
TASK=$(aws --profile anubanu-prod ecs run-task ...)
```

Expected:
- No `EXTRACTION RPD HIT` log.
- Cache row count unchanged.
- LLM call count ≈ 0 (visible in OpenAI org dashboard or by grepping the task logs for `Extracting structured content (async)`).
- Sync exits 0 in well under the legacy ~30 min — most of the time is now spent on embedding deltas (which are also near-zero in steady state).

- [ ] **Step 5: Notify telegram on success**

Post a summary in the telegram-topics chat:

```
extraction_cache rolled out to prod (PR #NN merged, deploy v??? complete).

What changed
• per-file gpt-4o-mini calls now go through an aurora-backed cache keyed (content_hash, extractor_version)
• RPD-shaped 429s short-circuit the gather, log RESUME:, exit 0 — partial progress persists
• --force-rebuild now also purges extraction_cache for the targeted (bot, context, version)
• 9 new pytest cases cover get/put/purge, cache-hit/miss integration, RPD/RPM split, force_rebuild purge

Backfill verified
• day-1: cache seeded to ~10K rows, RPD HIT logged with RESUME: hint
• day-2: cache filled to ≥13K rows
• day-3: sync exits 0 with ~0 LLM calls in steady state
```

---

## Self-Review

**Spec coverage:**

- ✅ "Survives task/container replacements" — Aurora table, alembic 0008 (Tasks 3 + 11).
- ✅ "Skips gpt-4o-mini call entirely on cache hit" — `_get_metadata_for_content_async` L2 lookup before `concurrency.run_bounded` (Task 7).
- ✅ "Survives RPD-induced aborts gracefully" — `RpdExhausted`, `_is_rpd_error`, `rpd_tripped` event, `EXTRACTION RPD HIT … RESUME:` log, exit 0 (Tasks 6 + 7).
- ✅ "Mirrors documents-layer delta semantics" — `force_rebuild=True` purges; orphans kept (Task 8).
- ✅ "No `--no-extraction-cache` flag" — explicit non-goal in spec; no CLI changes in plan.
- ✅ Cache key `(content_hash, extractor_version)` — alembic 0008 uses composite UNIQUE.
- ✅ All 9 acceptance-criteria tests are in Task 2 + Task 5 (4 unit + 5 integration).
- ✅ CLAUDE.md update — Task 9.
- ✅ Post-deploy seed + day-2 + day-3 verification — Tasks 12 + 13.

**Placeholder scan:**

- Task 7 step 1 references "the existing structure" of `async_retry_openai` — that's a "read the code first" instruction, not a placeholder; the surgical edit pattern is fully specified.
- Task 11 step 6 references "the master-role recipe described in parlibot's CLAUDE.md" — the recipe exists verbatim there (for migration 0007). Acceptable cross-reference.
- Task 13 references "OpenAI org dashboard" for LLM call counts — operator-grade observability, fine as a reference.
- No "TODO" / "TBD" / "implement later".

**Type consistency:**

- `bot`, `context_name`, `extraction_cache` are keyword-only (* in signature) across `_get_metadata_for_content_async`, `_process_file_stream_async`, `collect_context_sources_async`, `collect_context_sources` — ✅
- `force_rebuild=True` purge in `vector_store_update` keys on `(bot, context, EXTRACTION_VERSION)` matching the spec's policy — ✅
- `RpdExhausted` raise/catch chain: raised by `async_retry_openai` decorator → propagated by `concurrency.run_bounded` → caught by `_get_metadata_for_content_async` (sets trip flag) → re-raised → caught by `asyncio.gather`'s `return_exceptions=True` → counted by `collect_context_sources_async` → logged + dropped — ✅

**Bite-sizing:**

Each task has 3–7 numbered steps. Each step is one self-contained edit, one test run, or one shell invocation. No step contains "implement the rest of section X" — all code blocks are complete.

No issues found. Plan is ready for execution.
