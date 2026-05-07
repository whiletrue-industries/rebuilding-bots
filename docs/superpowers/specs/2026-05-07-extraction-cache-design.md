# Aurora-Backed Extraction Cache for `dynamic_extraction`

**Status:** draft, awaiting review
**Date:** 2026-05-07
**Author:** Claude Code (paired with @amir)

## Problem

`botnim sync` runs a per-file `gpt-4o-mini` LLM call inside `dynamic_extraction.py` to derive structured metadata (title, summary, legal references, topics, …) for every source file collected by `collect_context_sources`. The call lives in `_get_metadata_for_content_async` at `botnim/collect_sources.py:87`, gated by an asyncio semaphore and decorated with `async_retry_openai` for 429s.

Two layers of trouble compound on the `knesset_protocols` context:

1. **Volume.** `knesset_protocols` is a CSV-typed context that yields one synthetic `.md` per CSV row. As of 2026-05-07 that's ~13K rows. Sync therefore queues ~13K `gpt-4o-mini` calls per run.
2. **Cache durability.** A cache layer already exists — `kvfile.kvfile_sqlite.CachedKVFileSQLite` keyed on `sha256(content.strip())[:16]` — but it sits at `<repo>/cache/metadata/` *inside the container's writable layer*. Every ECS task replacement rewinds the cache to whatever (empty) state the Docker image was built with. So on every fresh task, every sync is fully cache-cold for `dynamic_extraction`.

The two together produce the observed failure mode: `gpt-4o-mini` RPD on the prod org is 10K/day; sync starts, burns through ~10K extractions in the first minutes, then the org-level RPD limit returns 429s on every subsequent extraction. `async_retry_openai` retries 6 times with exponential backoff up to 60s — useless against RPD which doesn't reset until midnight UTC — then bubbles. `asyncio.gather` aborts. **No partial progress is persisted**: the next sync run starts at file 0 because (a) the in-container KVFile is gone after the task replacement that follows the sync failure, and (b) `documents` rows are only ever inserted *after* extraction succeeds, so even if the embed-side delta-sync is intact, it has nothing to do because we never got to embed anything.

The delta-sync work shipped on 2026-05-06 (PR #128) made the *embed side* of `vector_store_update` cheap-on-rerun: each chunk's content_hash is checked against `documents` and skipped if present. But the LLM extraction in `collect_context_sources` runs **before** that skip — every file pays one `gpt-4o-mini` call regardless. Delta-sync is decisive for embeddings, useless for extraction.

## Goal

A single durable extraction cache that:

1. **Survives task/container replacements** by living in Aurora, not the container filesystem.
2. **Skips the `gpt-4o-mini` call entirely** when the same content was extracted before at the same prompt+model version.
3. **Survives RPD-induced aborts gracefully**: when the daily quota is exhausted, the run drains in-flight tasks, persists whatever it managed to extract, prints a clear RESUME message, and exits 0. Re-running sync the next day picks up from where it stopped — the second run's cost is bounded by the number of files that didn't get extracted on the first run.
4. **Mirrors the documents-layer delta semantics**: read-through cache by default; `--force-rebuild` purges the cache rows for the selected contexts so the next run re-extracts; orphans (cache rows whose source no longer exists upstream) are left in place.

Non-goals for this spec:

- Replacing the existing in-container KVFile cache for non-Aurora deployments. Aurora is the only durable store the prod ECS path has; the legacy KVFile is no longer the system of record but is left in place as a per-process L1 (within a single sync run, two files with identical content shouldn't pay two Aurora roundtrips). It can be removed in a followup once we're confident.
- A separate cache for the PDF Stage-2 extractor (`process_pdfs.py` field_extraction). That's a different cache layer covered by an earlier draft (an `extractions` table). The two designs are complementary: this spec covers the `dynamic_extraction.py` per-`.md` metadata cache; the other spec covers the per-PDF field cache. We pick distinct table names to keep them independent.
- Rate-aware scheduling (e.g. detecting RPD remaining and pacing the run to land just under the limit). RPD is exposed by OpenAI as response headers; consuming them is a useful followup but is more code than this spec needs to ship the resume property.
- A `--no-extraction-cache` CLI escape hatch. **Decision (deferred per YAGNI):** operators who need to force re-extraction can either bump `EXTRACTION_VERSION` (the right answer when the prompt/model actually changed) or run `--force-rebuild --replace-context <slug>` (the right answer when they want to re-extract one context). A separate "bypass cache reads but still write to it" mode is not justified by any current operator workflow; if a need surfaces we can add it cheaply later.
- Pre-commit lint forcing operators to bump `EXTRACTION_VERSION` when the prompt changes. Useful, separate.

## Decisions taken inline

These five decisions were called out in the planning brief; recording the picks and rationale here so the implementation has no ambiguity:

1. **Cache key = `(content_hash, extractor_version)`.** `content_hash` alone (matching the documents layer) was the user's first preference. We extend with `extractor_version` because the cached payload encodes a JSON schema (the `_DEFAULT_TEMPLATE` shape) plus the model's interpretation of a specific prompt — both of which can change without the *content* changing. A schema drift would silently feed stale-shape payloads into `documents.metadata` and into the unified bot's tool responses; bumping a version constant is one line of code and gives a clean kill-switch. The cost is one extra `TEXT NOT NULL` column and one composite index; on a ~13K-row table that's negligible.
2. **Failure semantics = catch RPD mid-loop, log a `RESUME` line, exit 0 cleanly** (option (a) in the brief). The whole point of the cache is to make resume cheap; partial progress must be persisted, and the ECS deploy wrapper interprets non-zero exits as a failed migrate phase, which would cancel the deploy. Operators see `EXTRACTION RPD HIT` in logs and re-run sync the next day; the cache makes that re-run nearly free for already-extracted files. Bubbling and exiting non-zero (option (b)) would force every operator to know to re-run anyway, with the deploy now in a flapping-failed state. RPM 429s continue to retry as today (different limit, resets in seconds, decorator already handles it).
3. **Orphan handling = leave cache rows alone when their source disappears upstream.** Mirrors the documents-layer policy (PR #128) and the project's "delta default, force-rebuild for purge" model. A cache row whose content nobody asks for any more is a free row; storage cost is bounded (see Risks). Operators who care can run `--force-rebuild` per quarter or hit Aurora directly.
4. **`force_rebuild=True` purges `extraction_cache` for the selected `(bot, context, extractor_version)`** before re-extraction. Wiping `documents` without wiping `extraction_cache` would be confusing: the next run would re-embed from cached extractions that the operator implicitly wanted re-derived. Keep the two layers in lockstep.
5. **No `--no-extraction-cache` CLI flag.** YAGNI per the brief. The two existing escape hatches (`EXTRACTION_VERSION` bump, `--force-rebuild`) cover the realistic operator needs. Adding a third bypass-but-write mode is more surface area for tests, docs, and operator confusion than it's worth; we can add it in a followup if a real workflow surfaces.

## Design

### New Aurora table: `extraction_cache`

Alembic migration `0008_extraction_cache.py`:

```sql
CREATE TABLE extraction_cache (
    id                 BIGSERIAL PRIMARY KEY,
    content_hash       TEXT        NOT NULL,        -- sha256(content.strip()) hex (full 64 chars)
    extractor_version  TEXT        NOT NULL,        -- bumps on prompt or model change
    payload            JSONB       NOT NULL,        -- the dict returned by extract_structured_content_async
    bot                TEXT        NOT NULL,        -- recorded for purge + observability
    context            TEXT        NOT NULL,        -- recorded for purge + observability
    document_type      TEXT,                        -- recorded for observability (e.g. 'text/markdown')
    extracted_at       TIMESTAMPTZ NOT NULL DEFAULT now(),

    UNIQUE (content_hash, extractor_version)
);

CREATE INDEX idx_extraction_cache_purge
    ON extraction_cache (bot, context, extractor_version);
```

Key choices, with rationale:

- **Lookup key is `(content_hash, extractor_version)`** — not `(bot, context, …)`. Two contexts that happen to ingest the same raw text (a quote of a Knesset minute pasted into an article and into a protocol, or two contexts pointing at the same upstream CSV) share one cache row. This is a real win for `plenary_schedule` ↔ `knesset_protocols` overlap and for any future context that re-uses public-domain text.
- **`content_hash` is a full sha256 hex** (64 chars), not the 16-byte prefix the legacy KVFile used. Prefix collisions at 13K rows are still vanishingly improbable, but full sha256 costs us nothing and matches what `documents.content_hash` stores.
- **`extractor_version` is a single string** (e.g. `"v1-gpt-4o-mini"`) defined as a module-level constant `EXTRACTION_VERSION` in `botnim/dynamic_extraction.py`. Bump it whenever:
  - `_DEFAULT_TEMPLATE` (the JSON schema) changes
  - `_build_system_message` (the prompt body) changes
  - The model in `_async_chat_completion` changes
  - Any post-processing in `_parse_response_content` changes the output dict shape
  Don't bump for unrelated bug fixes (retry wiring, logging). A future pre-commit lint that warns on prompt/model edits without a bump is out of scope.
- **`bot` and `context` are stored but NOT part of the unique key.** They exist purely so we can `DELETE WHERE bot=? AND context=?` cheaply (the `idx_extraction_cache_purge` index supports this) when an operator runs `--force-rebuild`. If two contexts share a row, force-rebuilding one doesn't purge the row — the other context is still using it. (Implementation: store the bot/context of the *first* writer; subsequent writers `DO UPDATE SET extracted_at=now()` but leave bot/context alone. Acceptable because the column's only consumer is the purge query, which is itself a coarse op.)
- **`document_type`** is stored for observability (it's already a known field passed through `_get_metadata_for_content_async`). Not used in the lookup. Useful when grepping the cache to debug extractor regressions.

### New helper module: `botnim/extraction_cache.py`

```python
class ExtractionCache:
    """Aurora-backed read-through cache for dynamic_extraction outputs.

    Lookup key: (content_hash, extractor_version).
    Concurrency-safe via INSERT … ON CONFLICT.
    """
    def __init__(self, environment: str): ...

    def get(self, content_hash: str, extractor_version: str) -> dict | None:
        """Return cached payload dict or None. Single-row roundtrip."""

    def put(self, content_hash: str, extractor_version: str, *,
            payload: dict, bot: str, context: str, document_type: str | None) -> None:
        """Idempotent upsert. ON CONFLICT (content_hash, extractor_version)
        DO UPDATE SET payload = EXCLUDED.payload, extracted_at = now()."""

    def purge(self, bot: str, context: str,
              extractor_version: str | None = None) -> int:
        """Delete rows for (bot, context). Returns row count.
        If extractor_version is given, scope further to that version."""
```

Implementation details:

- Reuses `botnim/db/session.py:get_session()` — same engine, same connection-pool, same env-var convention as the rest of Aurora code. No new env vars.
- `get` is the hot path. One `SELECT payload FROM extraction_cache WHERE content_hash=? AND extractor_version=?`. Uses the unique index automatically. Returns `None` on miss; the caller falls back to the LLM call.
- `put` uses `INSERT … ON CONFLICT (content_hash, extractor_version) DO UPDATE SET payload = EXCLUDED.payload, extracted_at = now()`. Two concurrent sync workers extracting the same content both succeed; the last writer wins on payload (which is fine because they should be identical at the same `extractor_version`).
- `purge` is operator-grade. Returns `rowcount` for logging. Used by force-rebuild wiring.

Module size target: ~80 LOC plus tests.

### Refactored `dynamic_extraction.py` and `collect_sources.py`

A new module-level constant in `dynamic_extraction.py`:

```python
EXTRACTION_VERSION = "v1-gpt-4o-mini"  # bump on prompt/model/schema change
```

`extract_structured_content_async` is unchanged at its surface — it still takes `text, template, document_type` and returns the parsed dict. The cache integration happens one level up, in `collect_sources._get_metadata_for_content_async`, where the bot/context is known.

`collect_sources._get_metadata_for_content_async` is refactored to use `ExtractionCache` as the primary cache (the legacy KVFile becomes a within-process L1 only). The new ordering:

```python
async def _get_metadata_for_content_async(
    content, file_path, document_type, concurrency, *,
    bot, context_name, extraction_cache,
    client=None,
):
    content_hash = hashlib.sha256(content.strip().encode('utf-8')).hexdigest()

    # L1: per-process KVFile (legacy). Fast path within a single run.
    cached_local = _cached_metadata_for_content(content)
    if cached_local is not None:
        return cached_local

    # L2: Aurora cache (durable across task replacements).
    if extraction_cache is not None:
        cached_aurora = extraction_cache.get(content_hash, EXTRACTION_VERSION)
        if cached_aurora is not None:
            # Promote to L1 and return.
            async with concurrency.cache_lock:
                cache.set(_cache_key(content), {'content': content, 'metadata': cached_aurora})
            return cached_aurora

    # Miss. Acquire semaphore, call OpenAI, persist to both caches.
    try:
        extracted_data = await concurrency.run_bounded(
            extract_structured_content_async,
            content, document_type=document_type, client=client,
        )
    except RpdExhausted:
        # Special-cased below — see "RPD handling".
        raise
    except Exception as e:
        metadata = _build_metadata_record(content, file_path, document_type, None, e)
        # Don't cache errors; next run retries.
        return metadata

    metadata = _build_metadata_record(content, file_path, document_type, extracted_data, None)

    if extraction_cache is not None:
        try:
            extraction_cache.put(
                content_hash, EXTRACTION_VERSION,
                payload=metadata, bot=bot, context=context_name,
                document_type=document_type,
            )
        except Exception as e:
            # Aurora write failed — don't poison the run. Log, fall through to L1.
            logger.warning("extraction_cache.put failed for %s: %s", file_path, e)

    async with concurrency.cache_lock:
        cache.set(_cache_key(content), {'content': content, 'metadata': metadata})
    return metadata
```

Function signature gains: `bot`, `context_name`, `extraction_cache`. These flow from `collect_context_sources_async`, which gains the same params and threads them in. `collect_context_sources` (sync wrapper) gains them too.

`vector_store_base.vector_store_update` is updated to construct the cache once per run and pass it down through `collect_context_sources`. The `bot` slug comes from `self.config["slug"]`. `context_name` comes from `context_['slug']`.

**Confirming the "content_hash already exists before extraction" question:** yes — the content is in hand (read from disk or from the upstream CSV) inside `_raw_streams_for_context` before any LLM call. Computing `sha256(content.strip())` adds ~µs per file. The legacy KVFile already does it (with a 16-char prefix) at line 31 of `collect_sources.py`. We re-use the same hash, just with the full 64-char hex. So the cache key is trivially derivable, no earlier hash plumbing required.

### RPD handling

A new exception type, `botnim.dynamic_extraction.RpdExhausted`, is raised by the inner OpenAI call when the response body indicates the daily-requests limit has been hit. The detection happens inside `_async_chat_completion` (or its retry wrapper) by inspecting the error message:

```python
def _is_rpd_error(exc: BaseException) -> bool:
    msg = str(exc).lower()
    return (
        "requests per day" in msg
        or "rpd" in msg.split()         # word-boundary match
        or "daily limit" in msg
    )
```

Justification: OpenAI's 429 body for RPD includes a phrase like `"You exceeded your current quota, please check your plan and billing details. … Limit: requests per day"`. RPM 429s say `"Rate limit reached for requests"` and pass through the existing retry. We only short-circuit when we're sure it's the daily limit.

The retry decorator `async_retry_openai` is updated: if a 429 is detected AND `_is_rpd_error(exc)` is True, raise `RpdExhausted(exc)` immediately (no retries — the limit doesn't reset until midnight UTC, retrying within this run is pointless).

`collect_context_sources_async` is updated to catch `RpdExhausted` from `asyncio.gather` and short-circuit:

```python
results = await asyncio.gather(*tasks, return_exceptions=True)
file_streams = []
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
        "RESUME: re-run `botnim sync %s %s` after the daily limit resets "
        "(midnight UTC). Cached extractions persist in Aurora; the next run "
        "will only call gpt-4o-mini for the remaining %d files.",
        rpd_count, len(tasks), context_name,
        len(file_streams), environment, bot, rpd_count,
    )
```

The crucial point: **the run does not abort.** It returns the partial `file_streams` list. `vector_store_update` then proceeds to embed those into Aurora as if nothing was wrong. The next run reads cache hits for everything that was extracted (zero `gpt-4o-mini` calls for those), and pays the LLM cost only for the `rpd_count` leftovers. After enough re-runs, the cache is fully warm.

We also short-circuit *within* a context once we've seen one RPD error, to avoid burning the remaining `asyncio.gather` tasks on guaranteed-to-fail calls. Implementation: a shared `asyncio.Event` (`rpd_tripped`) on `SyncConcurrency`. The first task to hit RPD sets it. Subsequent tasks check it before acquiring the semaphore and short-circuit to `RpdExhausted` without calling OpenAI. Calls already in-flight finish naturally (they may all hit RPD too, all raise — that's fine, all get counted).

Exit code: 0 on partial-RPD completion. The deploy.sh phase 8 wrapper interprets exit 0 as success, which is what we want — the caller can see the count in logs but won't fail-out the deploy. Operators can grep logs for `EXTRACTION RPD HIT` to see how many files are still pending.

### `--force-rebuild` purges `extraction_cache` for selected contexts

When the operator passes `--force-rebuild` with a specific `--replace-context <slug>`, the `vector_store_update` flow today calls `get_or_create_vector_store(force_rebuild=True)` which `DELETE FROM documents WHERE context_id=cid`. We extend this to also purge `extraction_cache` rows for that `(bot, context)` at the *current* `EXTRACTION_VERSION`:

```python
if should_force_rebuild:
    if extraction_cache is not None:
        purged = extraction_cache.purge(
            bot=self.config['slug'],
            context=context_name,
            extractor_version=EXTRACTION_VERSION,
        )
        logger.info("Purged %d extraction_cache rows for %s/%s @ %s",
                    purged, self.config['slug'], context_name, EXTRACTION_VERSION)
```

Scoping the purge to the current `EXTRACTION_VERSION` (rather than all versions) means rows from older versions remain — harmless (they'll never be read) and cheap to leave (they get superseded when an operator wants to vacuum manually).

Without `--force-rebuild`, the cache is preserved across runs — the whole point of this spec.

### Behavior matrix

| Scenario | `extraction_cache` rows | LLM calls | Notes |
|---|---|---|---|
| First-ever sync of `knesset_protocols` (~13K) on a fresh env | 0 → up to 10K in one run (RPD) | up to 10K | RPD trips, partial results persist |
| Second sync the next day | ~10K → ~13K | ~3K (the remainder) | Second day finishes the backfill |
| Third sync once cache is warm | ~13K (no growth) | 0 | Steady-state: zero LLM cost |
| Fourth sync after a single new row added upstream | ~13K + 1 | 1 | Only the new row pays |
| Sync with `--force-rebuild --replace-context knesset_protocols` | purged → ~13K | ~13K (subject to RPD) | Same recovery loop as fresh env |
| Operator bumps `EXTRACTION_VERSION` (prompt edit) | 13K old + 13K new (orphans kept) | ~13K (subject to RPD) | Same recovery loop; old version rows can be vacuumed manually if storage matters |

## Concurrency

Two scenarios:

1. **Two sync runs racing.** Each runs its own `asyncio.gather`. Both miss for the same content_hash; both call OpenAI; both call `extraction_cache.put`. The `INSERT … ON CONFLICT … DO UPDATE` makes the second write idempotent. Cost: one extra LLM call per double-extraction. No row-level race, no duplicate rows.
2. **Sync racing fap.** Fap writes the per-context CSV; sync reads it. The atomic-rename pattern in fap is unchanged; sync sees the old or new CSV but never a torn one. The cache is content-addressed, so even if sync reads the old CSV and fap concurrently writes a new one, the cache rows for the old content are reused by future syncs (until the operator runs `--force-rebuild` or bumps `EXTRACTION_VERSION`).

No locks are needed for correctness. The existing `concurrency.cache_lock` continues to guard the L1 KVFile writer.

## Tests

New test file `tests/test_extraction_cache.py` (pytest-postgresql fixture, real Aurora schema):

1. `test_get_returns_none_on_miss` — empty table; assert `get('h', 'v')` returns `None`.
2. `test_put_then_get_roundtrips_payload` — insert via `put`, fetch via `get`, assert dict equality including nested fields.
3. `test_put_is_idempotent_on_conflict` — call `put` twice with same key, different payload; second call wins; one row total.
4. `test_purge_scopes_to_bot_context_version` — insert 4 rows across two `(bot, context)` and two versions; purge one combo; assert only that combo's rows are gone.
5. `test_collect_uses_aurora_cache_hit` — pre-populate one row; mock OpenAI to raise (assert never called); run `_get_metadata_for_content_async`; assert payload matches.
6. `test_collect_writes_aurora_cache_on_miss` — empty cache; mock OpenAI to return a known dict; run; assert one row written with the expected `(content_hash, extractor_version, bot, context)`.
7. `test_rpd_error_short_circuits_and_returns_partial` — mock OpenAI to raise an RPD-shaped error after the 3rd call (in a 5-task gather); assert `file_streams` has the first 3 (cache+success), last 2 are dropped, `rpd_count==2`, `EXTRACTION RPD HIT` is logged with the resume hint, run exits without raising.
8. `test_rpm_error_still_retries_via_decorator` — mock OpenAI to raise an RPM-shaped 429 once then succeed; assert one retry sleep, one final success, no `RpdExhausted`.
9. `test_force_rebuild_purges_then_writes` — pre-populate; run with `force_rebuild=True`; assert pre-existing rows for that `(bot, context, version)` are gone after the run; new rows written for the new file_streams.

Existing tests in `tests/collect_sources/` and `tests/vector_store/test_aurora_delta.py` are unchanged in surface and should pass — they use the L1 KVFile and a fake OpenAI client; the new code path is disabled when `extraction_cache=None` (the default in those test fixtures).

## Caller updates

- **`botnim/sync.py:sync_agents`** — passes a single constructed `ExtractionCache(environment)` down to `_sync_vector_store` → `vector_store_update`. No new CLI flag needed (per the YAGNI decision).
- **`botnim/vector_store/vector_store_base.py:vector_store_update`** — constructs one `ExtractionCache(environment=...)` instance per run; passes to `collect_context_sources` along with `bot=self.config['slug']`.
- **`botnim/collect_sources.py`** — `collect_context_sources` and `collect_context_sources_async` accept `bot, extraction_cache`; thread through.
- **`botnim/cli.py`** — no new flag.
- **`deploy.sh`** — no change. `botnim sync prod all --backend aurora` continues to invoke the new default behavior; the cache reads/writes happen automatically.

## Operator-facing surface

- `/admin/sources` — no change. The cache layer is invisible to admin UIs in v1.
- New CLI commands deferred to a followup if useful: `botnim cache stats`, `botnim cache purge`. Operators can hit Aurora directly for now (`SELECT bot, context, extractor_version, count(*) FROM extraction_cache GROUP BY 1,2,3`).

## Risks and mitigations

| Risk | Mitigation |
|---|---|
| RPD detection misses a real RPD error and we burn through retries fruitlessly | Detection is conservative (3 substring checks). Worst case we retry up to 6 times then bubble. Add a unit test covering the exact OpenAI RPD body shape; bump detection if it changes. |
| RPD detection false-positives on RPM and we short-circuit prematurely | The substring `"requests per day"` is specific; RPM errors say `"requests per minute"`. Test `test_rpm_error_still_retries_via_decorator` covers this. |
| Two contexts share a content_hash, we purge one, the other goes stale | By design the purge is scoped to `(bot, context, version)`. The shared row stays. The other context still reads it. If an operator wants a global purge, they can run `--force-rebuild` for every context, or hit the table with raw SQL. |
| Aurora write contention slows sync | Each `put` is one row, ~1ms over the connection pool. Negligible vs. a 1–3s `gpt-4o-mini` call. |
| `EXTRACTION_VERSION` discipline drifts (operator changes prompt without bumping) | Document in CLAUDE.md. Followup: pre-commit lint that diffs `_DEFAULT_TEMPLATE` and `_build_system_message` and warns on a missing bump. |
| Aurora is briefly unreachable mid-sync | `extraction_cache.put` failures are logged + swallowed (don't poison the run); the L1 KVFile still gets the value for within-run reuse; the next run re-extracts those files (acceptable rare blip). |
| Backfill cost: warming the cache from cold on prod | Bounded by RPD. ~13K knesset_protocols + ~few-hundred others = ~14K calls = ~2 days at 10K RPD. Each day exits 0 with the resume message; on day 3 onward, the cache is warm and runs cost ~0 LLM calls in steady state. |
| Cache table grows unbounded | Bounded by upstream content. Even at 50K rows × 5KB JSONB = 250MB. Trivial vs. `documents` table. No GC needed. |
| Operator wants to bypass the cache for a one-off debug run, no flag exists | Two existing escape hatches: bump `EXTRACTION_VERSION` (right answer when prompt/model changed) or `--force-rebuild --replace-context <slug>` (right answer when re-extracting one context). YAGNI on a third flag until a real workflow demands it. |

## Acceptance criteria

- [ ] Alembic 0008 applies cleanly on staging and prod.
- [ ] `tests/test_extraction_cache.py` (9 cases) all pass.
- [ ] After cold-cache prod sync: `extraction_cache` table has between 8K and 10K rows; sync exits 0; logs include `EXTRACTION RPD HIT … RESUME: …`.
- [ ] After warm-cache prod sync (next day): `extraction_cache` row count climbs to ≥13K; sync exits 0; logs show `gpt-4o-mini` call count well under 5K (the remainder of knesset_protocols + non-cached deltas elsewhere).
- [ ] After steady-state prod sync (day 3+): `gpt-4o-mini` call count == 0 in the absence of upstream content changes.
- [ ] `--force-rebuild --replace-context knesset_protocols` deletes the `(bot=unified, context=knesset_protocols, extractor_version=v1-gpt-4o-mini)` rows; the next sync re-extracts them.
- [ ] CLAUDE.md (rebuilding-bots) updated: new row in the `botnim sync` modes table covering extraction-cache behavior, plus a short subsection covering the resume semantics and the version-bump policy.

## Out of scope (followups)

- `botnim cache stats|purge` CLI commands.
- `/admin/cache` admin UI.
- Pre-commit lint that requires `EXTRACTION_VERSION` bump on prompt/model edits.
- RPD-aware pacing (read OpenAI rate-limit response headers, slow down when remaining is low).
- Removing the legacy KVFile L1 once Aurora cache is proven (probably one release after this ships).
- Generalizing the same pattern to the PDF Stage-2 field extractor (handled by a separate spec).
- A `--no-extraction-cache` CLI flag if a real operator workflow surfaces.
