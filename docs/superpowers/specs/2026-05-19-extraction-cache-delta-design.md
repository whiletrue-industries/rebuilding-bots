# Extraction-cache delta reads: smooth out the `EXTRACTION_VERSION` bump cost

**Status:** proposed, 2026-05-19
**Author:** Claude Code (paired with @amir)
**Predecessors:**
- [Aurora-backed extraction cache](./2026-05-07-extraction-cache-design.md) — introduced the `extraction_cache` table and the `(content_hash, extractor_version)` key.
- [Aurora migration design](../../../docs/superpowers/specs/2026-04-26-aurora-migration-design.md) — the embed-side `documents.content_hash` delta-sync that this design now mirrors for extraction.

## Problem

The extraction cache shipped on 2026-05-07 made steady-state syncs near-zero LLM cost: any chunk whose `(content_hash, extractor_version)` exists in Aurora is served from cache and never sees `gpt-4o-mini`. Bumping `EXTRACTION_VERSION` — which we did on 2026-05-13 to gate the Hebrew OCR-reversal fix — does exactly what the predecessor spec said it should: invalidates every prior payload at one stroke.

What the predecessor spec did *not* account for is the **cost shape** that creates. On 2026-05-19 the OpenAI dashboard recorded 23,480 `Responses and Chat Completions` requests against the prod org by 05:50 UTC — six hours into the daily refresh. CloudWatch on `botnim-api-prod-api` logged **23,496 `Extracting structured content` lines** in the same window. The two numbers match 1:1: today's prod chat-completion budget went almost entirely to re-extracting chunks whose v1 cache rows are still in the table but no longer match the current `extractor_version`.

This isn't a bug in the cache — the cache is doing what was designed. It's a property of the design: a version bump trades one big lump of LLM cost for a clean kill-switch. The lump landed on the daily refresh and has been chipping through the backlog at the `gpt-4o-mini` RPD cap each day since 2026-05-13. Six days in, we're still re-extracting (the `knesset_protocols` context alone has ~13K rows; both staging and prod went through the bump independently).

The actual operator pain isn't paying the bump cost once. It's that **operators now hesitate to bump `EXTRACTION_VERSION` at all** because the bump triggers an unbounded multi-day surge of LLM calls that blocks the daily refresh from completing in its scheduled window. A legitimately-needed bump (prompt fix, schema change, preprocessing fix like the OCR gate) becomes a multi-day operational event instead of a one-line code change. This is exactly the kind of cost-shape friction that ends with stale extraction logic lingering in prod because nobody wanted to be the one to bump.

## Goal

A delta read path for `_get_metadata_for_content_async` that:

1. **Returns immediately on a version bump** by falling back to an older `extractor_version` row for the same `content_hash`. Sync's critical path is never blocked by a version-bump re-extraction surge.
2. **Re-warms the cache to the new `extractor_version` in the background**, bounded by a per-run budget so total daily LLM cost stays predictable regardless of corpus size.
3. **Is observable** — operators can see at any time how many rows are still on a stale version, and how fast re-warming is progressing.
4. **Preserves the existing kill-switch semantics**: bumping `EXTRACTION_VERSION` still means "every consumer eventually sees the new payload"; it just stops meaning "every consumer pays the new cost on day one".
5. **Does not change `--force-rebuild` semantics.** Operators who want today's behavior (purge + immediate re-extraction at the new version) keep getting it.

Non-goals:

- **A separate `--no-extraction-cache` CLI flag.** YAGNI as in the predecessor spec.
- **Rate-aware scheduling against OpenAI's response headers.** Useful followup, larger scope.
- **Auto-purge of stale rows after re-warm completes.** Storage cost is negligible (~kB per row) and an out-of-band cleanup is easier to operate than online deletes; the existing `--force-rebuild` path already covers it for operators who want it.
- **Cross-context fallback (serve a v1 row for context A as the fallback for context B's miss).** The cache is already keyed on `content_hash`, not bot/context, so this works for free today. No additional design needed.
- **A migration path for a future v3 bump.** This design's invariants hold for any number of versions: the fallback always picks the most-recent older version present.

## Decisions taken inline

1. **Fallback selection = "most-recently-extracted older version" — not "v1 specifically".** When v3 ships eventually, a v2 row is a better fallback than a v1 row (closer to current). Implementing this as `ORDER BY (extractor_version = $current) DESC, extracted_at DESC LIMIT 1` is one SQL clause and is robust across an arbitrary version chain.
2. **Re-warm is best-effort, not transactional.** The stale payload is served to the sync caller *before* the re-warm task is scheduled. If the re-warm fails (RPD, transient 5xx, parse error), the cache stays at the older version; the next run sees it as stale again and may retry. Failures log + continue — they never poison the sync result. This mirrors how the existing `extraction_cache.put` failure handling already works (line 240-241 of `collect_sources.py`).
3. **Budget is per-`SyncConcurrency`, not per-process.** Each sync orchestrator owns its own budget. Two parallel syncs in the same env (shouldn't happen — advisory lock prevents it — but the predecessor spec leaves room for it) would each get their own budget; the cache itself enforces no global throttle. Acceptable because the advisory lock already prevents concurrent syncs.
4. **Default budget = `2000`.** Sized so a 13K-row corpus (the `knesset_protocols` worst case) fully re-warms in ~7 days when the daily refresh fires once per env. Tunable via env var `EXTRACTION_REWARM_MAX_PER_RUN`. Setting it to `0` disables re-warming entirely (the cache stays stale forever — useful for read-mostly disaster-recovery envs). Setting it very high reproduces today's "warm everything at once" cost shape.
5. **Stale-served payloads count toward the budget too — but only when re-warm is *attempted*, not when re-warm is *skipped*.** Distinction: if budget is 0 or already exhausted, the stale read costs nothing and we serve indefinitely. If budget is available, we schedule a re-warm task — that task counts against the budget regardless of whether it succeeds. This means a run with budget=2000 will produce at most 2000 LLM calls in the worst case (every re-warm task succeeds and counts), not 2000 net successful re-extractions.
6. **No schema change to `extraction_cache`.** The fallback is a query-shape change, not a data-shape change. The existing unique key `(content_hash, extractor_version)` continues to hold; multiple version rows for the same `content_hash` already coexist (see "Backfill" below). The table picks up additional metadata only if we choose to add a `rewarm_attempts` counter, which is itself optional for v1.
7. **Backward-compatible cutover.** The change is read-only-side; the existing writers continue to write at the current `EXTRACTION_VERSION`. Deploying this code into a cluster where the cache table contains only current-version rows is a no-op (every lookup is an exact hit; the fallback path is never taken). Deploying into the current cluster — where v1 + v2 coexist — immediately starts serving v1 stale-hits for the unwarm portion of the corpus.

## Design

### Read path (changes in `botnim/collect_sources.py`)

Replace the current L2 lookup at lines 202-211 of `_get_metadata_for_content_async`:

```python
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
```

with:

```python
# L2: Aurora cache (with version fallback for bump-day cost smoothing).
if extraction_cache is not None:
    try:
        hit = extraction_cache.get_with_fallback(content_hash, EXTRACTION_VERSION)
    except Exception as e:
        logger.warning("extraction_cache.get_with_fallback failed for %s: %s", file_path, e)
        hit = None
    if hit is not None:
        payload = hit["payload"]
        async with concurrency.cache_lock:
            cache.set(_cache_key(content), {"content": content, "metadata": payload})

        if hit["stale"] and concurrency.rewarm_budget_take():
            # Best-effort background re-extract at current EXTRACTION_VERSION.
            # Stale payload is already in flight to the caller; rewarm
            # failures must not affect this run.
            asyncio.create_task(_rewarm_extraction(
                content=content,
                content_hash=content_hash,
                file_path=file_path,
                document_type=document_type,
                bot=bot,
                context_name=context_name,
                extraction_cache=extraction_cache,
                concurrency=concurrency,
                client=client,
            ))

        return payload
```

The fast-path (exact hit) costs the same one Aurora roundtrip as today. The stale-hit case adds at most one `asyncio.create_task` scheduling — no extra LLM calls on the critical path. The re-warm task itself goes through `concurrency.run_bounded` exactly like a normal LLM extraction would, so it competes for the same async semaphore and the same `async_retry_openai` handling.

### `extraction_cache.get_with_fallback()`

New method on `ExtractionCache` (`botnim/extraction_cache.py`):

```python
def get_with_fallback(
    self, content_hash: str, current_version: str
) -> dict | None:
    """Return {"payload": dict, "from_version": str, "stale": bool} or None.

    Prefers an exact match at `current_version`; if absent, returns the
    most-recently-extracted row at any other version. Single roundtrip.
    """
    with get_session() as sess:
        row = sess.execute(text(
            "SELECT payload, extractor_version "
            "FROM extraction_cache "
            "WHERE content_hash = :h "
            "ORDER BY (extractor_version = :v) DESC, extracted_at DESC "
            "LIMIT 1"
        ), {"h": content_hash, "v": current_version}).fetchone()
    if row is None:
        return None
    payload = row[0] if isinstance(row[0], dict) else json.loads(row[0])
    from_version = row[1]
    return {
        "payload": payload,
        "from_version": from_version,
        "stale": from_version != current_version,
    }
```

The `ORDER BY (extractor_version = :v) DESC, extracted_at DESC LIMIT 1` clause is the entire fallback logic: PostgreSQL evaluates the boolean expression first (exact match wins), then breaks ties by recency. No CTE, no UNION, no extra index needed — the existing primary key on `(content_hash, extractor_version)` already gives a fast prefix scan on `content_hash`, and there are typically only 1-2 rows per hash so the sort cost is trivial.

The existing `get(content_hash, extractor_version)` method stays as-is for callers that want exact-only semantics (the `--force-rebuild` purge logic in `vector_store_base.py` does not need fallback).

### Orchestrator drains pending re-warm tasks before returning

Empirical finding from the 2026-05-19 local-compose validation: `asyncio.run()` (the entry point under `run_async()` in `botnim/_concurrency.py`) closes the event loop on return and **cancels** any pending tasks. A naive `asyncio.create_task(_rewarm_extraction(...))` is therefore cancelled mid-LLM-call when the main `asyncio.gather` returns, leaving `rewarmed_count=0` even though the budget was consumed.

Fix: track each scheduled re-warm task on the concurrency object (`rewarm_tasks: list[asyncio.Task]`) and `asyncio.gather(*tasks, return_exceptions=True)` them at the end of `collect_context_sources_async`, after the primary extraction `gather` and before the function returns. Each re-warm runs through `concurrency.run_bounded` so it competes for the same async semaphore; the drain just makes sure the loop stays open until they finish.

Penalty: the run blocks for as long as the slowest in-flight re-warm. With `EXTRACTION_REWARM_MAX_PER_RUN=2000` and a 10-wide semaphore, that's ~200 sequential LLM calls per pool slot, ~3 minutes of tail latency at the end of a refresh. Acceptable for daily-cadence refreshes; trivial for the typical 5-50 re-warms per context.

### Note on L1 (KVFile) interaction with stale payloads

When L2 returns a stale-hit, the existing L1 KVFile write at the same line of `_get_metadata_for_content_async` populates L1 with the stale payload — exactly as the spec intended for fast intra-run reads. **A subsequent extraction of the same content in the same process therefore hits L1 and never re-consults L2** (and thus never re-triggers the budget). This is acceptable: each ECS task replacement empties L1 (it lives at `/srv/cache/metadata.sqlite` in the writable container layer, not on EFS), so the next process boot will go through L2 again. Operators verifying the delta locally must `rm /srv/cache/metadata.sqlite` between successive test runs against the same content — covered in the manual-test recipe at the bottom of this spec.

### Re-warm budget on `SyncConcurrency`

New field on the existing `SyncConcurrency` class in `botnim/_concurrency.py`:

```python
class SyncConcurrency:
    def __init__(self, *, semaphore_size: int, rewarm_budget: int = 2000):
        ...
        self._rewarm_remaining = rewarm_budget
        self._rewarm_lock = asyncio.Lock()
        self.rewarmed_count = 0
        self.stale_served_count = 0

    async def rewarm_budget_take(self) -> bool:
        """Atomically decrement the re-warm budget. Returns True if a slot
        was available (caller should schedule re-warm), False otherwise
        (caller serves stale and skips re-warm)."""
        async with self._rewarm_lock:
            self.stale_served_count += 1
            if self._rewarm_remaining <= 0:
                return False
            self._rewarm_remaining -= 1
            return True
```

The lock is async because the call site is inside `_get_metadata_for_content_async` (an async function). Contention is trivial — the lock is held for two integer compares — but the lock matters because asyncio task-switching between the read and decrement could otherwise let a stale-flood race past the budget.

`stale_served_count` is incremented unconditionally on every stale-hit (not just budget-eligible ones), so the metric reflects "how much of this run was served stale" regardless of how aggressive the re-warm is.

### `_rewarm_extraction()` helper

Free function in `collect_sources.py`:

```python
async def _rewarm_extraction(
    *, content, content_hash, file_path, document_type,
    bot, context_name, extraction_cache, concurrency, client,
) -> None:
    """Re-extract `content` at the current EXTRACTION_VERSION and persist.
    Best-effort: failures log + continue without affecting sync result."""
    from .dynamic_extraction import (
        EXTRACTION_VERSION, RpdExhausted, extract_structured_content_async,
    )
    try:
        extracted = await concurrency.run_bounded(
            extract_structured_content_async,
            content, document_type=document_type, client=client,
        )
    except RpdExhausted:
        # Daily quota — let it propagate to the orchestrator's trip flag
        # so subsequent re-warms also short-circuit. The sync caller
        # already has its stale payload, so this RPD won't surface as a
        # sync failure.
        concurrency.rpd_tripped.set()
        return
    except Exception as e:
        logger.info("rewarm failed for %s: %s (stale payload remains)", file_path, e)
        return

    metadata = _build_metadata_record(content, file_path, document_type, extracted, None)
    try:
        extraction_cache.put(
            content_hash, EXTRACTION_VERSION,
            payload=metadata, bot=bot, context=context_name,
            document_type=document_type,
        )
        concurrency.rewarmed_count += 1
    except Exception as e:
        logger.info("rewarm cache.put failed for %s: %s", file_path, e)
```

The re-warm path deliberately does **not** update the L1 KVFile cache. The L1 was already populated with the stale payload when the sync caller was unblocked; overwriting it mid-run would create a race where two near-simultaneous lookups for the same content see different payloads. By the time the next sync runs, the L2 fallback will pick up the freshly-written current-version row and serve it as an exact hit; L1 starts cold next run anyway.

### Operator controls

| Knob | Effect | Where set |
|---|---|---|
| `EXTRACTION_REWARM_MAX_PER_RUN` (default `2000`) | Per-run cap on re-warm attempts. `0` = serve stale forever, never re-warm. | Env var on `botnim-api` task / local shell |
| `--force-rebuild` (existing) | Unchanged: purges `extraction_cache` for the targeted `(bot, context, current_version)`, then re-extracts. No fallback possible because purge erased the prior rows. | CLI flag |
| `--replace-context <slug>` (existing) | Unchanged: scopes processing to one context. | CLI flag |
| `botnim cache rewarm <bot> <context> [--version=<v>]` (**new, optional v2**) | Operator-driven full re-extraction of stale rows for one context, decoupled from the daily refresh. Honors RPD via the same `async_retry_openai` decorator. | New CLI subcommand (not in v1 scope of this spec — listed for completeness) |

The new CLI subcommand is deferred: v1 ships only the read-path change + budget + observability. If operators want to force a faster warm than the daily budget allows, `--force-rebuild --replace-context <slug>` already does it (at the cost of losing the fallback for that context).

### Observability

Three new log lines at the end of each sync run, in `vector_store_base.py` after `update_tool_resources/update_tools` completes:

```
EXTRACTION_CACHE_SUMMARY: bot=<slug> context=<slug>
  exact_hits=<N>  stale_served=<concurrency.stale_served_count>
  rewarmed=<concurrency.rewarmed_count>  llm_misses=<N>
  budget_remaining=<concurrency._rewarm_remaining>
```

Numbers are computed from counters that `_get_metadata_for_content_async` and `_rewarm_extraction` already maintain on `SyncConcurrency`; no additional plumbing.

For dashboards, a Cloudwatch Logs metric filter on `EXTRACTION_CACHE_SUMMARY` → metric `extraction_stale_served` makes the post-bump re-warm progress visible without needing a new DB column. A complementary SQL probe is also useful for ad-hoc checks:

```sql
SELECT extractor_version, count(*) AS rows, max(extracted_at) AS most_recent
  FROM extraction_cache
 WHERE bot = 'unified' AND context = 'knesset_protocols'
 GROUP BY 1
 ORDER BY rows DESC;
```

After full re-warm, all rows for the context should appear under the current `EXTRACTION_VERSION`. Until then, the row count under older versions shows the remaining stale backlog.

## Backfill / cutover

No data migration. The existing rows in `extraction_cache` are already in the right shape — the predecessor spec's `INSERT … ON CONFLICT (content_hash, extractor_version)` allows multiple version rows for the same `content_hash` to coexist, and that's exactly what enables the fallback.

The cluster as of 2026-05-19 has roughly:
- A partially-populated set of rows at `extractor_version = 'v2-gpt-4o-mini-heb-fix-ocr-gate'` (today's value), written by the daily refresh over the last 6 days.
- A larger set of legacy rows at `extractor_version = 'v1-gpt-4o-mini'` (pre-2026-05-13), still present because `EXTRACTION_VERSION` bumps don't purge.

Deploying this design as-is into that cluster means:
- Day 1: the first refresh after deploy serves v1 stale-hits for all chunks not yet at v2, schedules ~2000 background re-warms (those convert to v2 rows), and finishes its critical path in normal time. No multi-day surge.
- Subsequent days: the v1→v2 backlog drains at ~2000/day per env. The daily refresh continues to serve correctly throughout.
- Once all rows for an env are at v2, `stale_served=0` in the summary line and behavior is identical to today's steady state.

If for any reason an operator wants today's behavior post-deploy (immediate full re-extraction at the new version), they can:
1. Set `EXTRACTION_REWARM_MAX_PER_RUN=99999` for one refresh (reverts to "warm everything"), or
2. Run `botnim sync prod unified --force-rebuild` (purges v1 rows for the unified bot, forcing fresh extraction at v2 with no fallback).

## Risks

| Risk | Likelihood | Mitigation |
|---|---|---|
| Stale-served payloads are semantically older — a bot answer might cite a v1 extraction when v2 would have produced a more accurate one. | Medium during re-warm; near-zero in steady state. | `stale_served_count` is logged on every run and surfaces in the `EXTRACTION_CACHE_SUMMARY` metric filter. Operators have a clear escape hatch (`--force-rebuild`). Note that v1 was *correct enough* to ship for the entire pre-2026-05-13 corpus; the v1→v2 delta is the Hebrew OCR gate, which improves OCR'd Hebrew but doesn't change correctness for the (majority) non-OCR'd inputs. |
| Re-warm task explodes the event-loop with concurrent LLM calls and starves other operations. | Low. | Re-warm goes through `concurrency.run_bounded` exactly like a normal extraction — same semaphore, same retry decorator. The budget caps total scheduled re-warm tasks per run; the semaphore caps how many run concurrently. |
| Re-warm fails silently and stale rows stay stale forever, even when budget is available next run. | Low (logged at `info` level, surfaces in `EXTRACTION_CACHE_SUMMARY` as a slow-decreasing `stale_served`). | Operators monitor the summary line; if a row never warms, `--force-rebuild` is the kill-switch. Optional v2 follow-up: persist `rewarm_attempts` and `last_rewarm_attempt_at` on the v1 row so a stuck re-warm is observable per-row, not just in aggregate. |
| Budget exhaustion causes inconsistent state mid-run (some chunks warmed, others still stale). | Expected. | This is the design. Across multiple runs the cluster converges; within a single run, every chunk gets *some* valid payload. Sync's correctness is not affected — the unified bot's tool responses come from `documents`, not directly from `extraction_cache`. |
| Two parallel sync runs in the same env (advisory lock fails) double-burn the budget. | Very low (advisory lock has been reliable since shipped in PR #128). | Each run has its own budget instance; total LLM cost is `2 × budget` in the pathological case. Still bounded, still safe. |
| The fallback SQL query is more expensive than the current point lookup at very high cache-hit rates. | Low. | Plan analysis: existing PK on `(content_hash, extractor_version)` gives a fast index range scan on `content_hash`; the sort over at most 2-3 rows is negligible. EXPLAIN ANALYZE should be ~same μs as the current `WHERE content_hash = $1 AND extractor_version = $2`. If it ever isn't, an index on `(content_hash)` alone would force a cheaper sort. |

## Test plan

Unit-level (`tests/test_extraction_cache.py`):

1. `get_with_fallback` returns `{..., stale: False}` for an exact-version row.
2. `get_with_fallback` returns `{..., stale: True, from_version: 'v1-...'}` when only an older row exists.
3. `get_with_fallback` returns the *most recent* older row when multiple older versions exist (insert v1, v2, current=v3 missing; expect v2).
4. `get_with_fallback` returns `None` when no row exists for the `content_hash` at any version.

Integration (`tests/test_collect_sources_async.py` — new fixtures):

5. With L2 hit (exact): no LLM call, no re-warm scheduled, counters: `exact_hits=1`.
6. With L2 hit (stale) + budget=10: stale payload returned synchronously; one re-warm task scheduled; after `await asyncio.gather(...)`, cache has both v1 row (original) and new current-version row; counters: `stale_served=1, rewarmed=1`.
7. With L2 hit (stale) + budget=0: stale payload returned; no re-warm scheduled; counters: `stale_served=1, rewarmed=0`.
8. With L2 hit (stale) + budget=1, two stale chunks in the same run: first re-warm scheduled, second is not; counters: `stale_served=2, rewarmed=1`.
9. Re-warm RpdExhausted: stale payload still returned (no exception bubbles); `rpd_tripped` set; counters: `stale_served=1, rewarmed=0`; subsequent stale-hits in the same run still serve stale but skip re-warm scheduling because `rpd_tripped` is checked.
10. Re-warm failure (non-RPD exception): same as RpdExhausted but `rpd_tripped` not set; logged at `info`.

Backward compat:

11. With L1 KVFile hit: behavior unchanged (no L2 lookup, no re-warm).
12. With `--force-rebuild`: cache purge fires as today; no fallback rows survive for this `(bot, context, EXTRACTION_VERSION)`.

## Acceptance

Ship this design when, after merging and one full daily refresh in staging:

- `EXTRACTION_CACHE_SUMMARY` log line shows `stale_served > 0, rewarmed > 0, llm_misses` matches the count of truly-new chunks (not cached at any version). For the current cluster state, `stale_served` should be on the order of thousands and `llm_misses` should be on the order of dozens.
- Total `Extracting structured content` log lines drops from today's 23K-class number to a number roughly equal to `EXTRACTION_REWARM_MAX_PER_RUN + new_chunks` — i.e., `~2000` if no new corpus arrived.
- `/d/sources` and `/d/sanity` are unaffected (no contract change to `documents`, `context_snapshots`, or `sanity_runs`).
- Gold-set sanity DoD pass-rate on the next scheduled run is within noise of the pre-deploy baseline. Stale-served payloads should not move the rubric noticeably because the v1→v2 delta is the OCR gate, not the core extraction quality.

Subsequent days should show the stale-served count monotonically decreasing as the v1→v2 backlog drains, until it bottoms out at ~0 (steady state).

## Manual local-compose verification (recipe)

1. Bring up the aurora-local stack with a staging-cloned Aurora DB:

   ```
   docker compose -f docker-compose.aurora-local.yml up -d postgres botnim_api
   ```

2. Recreate a "post-bump" state for one small context by purging its v2 rows:

   ```
   docker exec botnim_api python -c "
   from botnim.db.session import get_session
   from sqlalchemy import text
   with get_session() as s:
       s.execute(text(\"DELETE FROM extraction_cache WHERE bot='unified' AND context='ידע רלוונטי על התקציב' AND extractor_version='v2-gpt-4o-mini-heb-fix-ocr-gate'\"))
       s.commit()
   "
   ```

3. Reset the `BIGSERIAL` sequence if the local DB came from a `pg_restore` dump (the dump doesn't carry the sequence state). Otherwise the rewarm's `INSERT` hits `duplicate key value violates unique constraint "extraction_cache_pkey"` on the id column:

   ```
   docker exec botnim_api python -c "
   from botnim.db.session import get_session
   from sqlalchemy import text
   with get_session() as s:
       m = s.execute(text('SELECT MAX(id) FROM extraction_cache')).scalar() + 1
       seq = s.execute(text(\"SELECT pg_get_serial_sequence('extraction_cache','id')\")).scalar()
       s.execute(text(f\"SELECT setval('{seq}', {m})\")); s.commit()
   "
   ```

4. Wipe the L1 KVFile so the test exercises L2 (subsequent runs against the same content hit L1 in-process):

   ```
   docker exec botnim_api rm -f /srv/cache/metadata.sqlite
   ```

5. Run extraction over the smallest local context with `EXTRACTION_REWARM_MAX_PER_RUN=5`:

   ```
   docker exec -e OPENAI_API_KEY=... -e OPENAI_API_KEY_STAGING=... \
               -e EXTRACTION_REWARM_MAX_PER_RUN=5 botnim_api \
     python -c "
   import yaml
   from pathlib import Path
   from botnim.collect_sources import collect_context_sources
   from botnim.extraction_cache import ExtractionCache
   with open('/srv/specs/unified/config.yaml') as f: spec = yaml.safe_load(f)
   ctx = next(c for c in spec['context'] if c['slug'] == 'common_budget_knowledge')
   cache = ExtractionCache(environment='local')
   collect_context_sources(ctx, Path('/srv/specs/unified'), bot='unified', extraction_cache=cache)
   "
   ```

Expected `EXTRACTION_CACHE_SUMMARY` line:

```
EXTRACTION_CACHE_SUMMARY: bot=unified context=common_budget_knowledge
    exact_hits=0 stale_served=45 rewarmed=5 llm_misses=0 rewarm_budget_remaining=0
```

Post-run, the cache should hold 45 v1 rows (unchanged) + 5 v2 rows (newly rewarmed). Re-running the same step (after re-purging L1) drains another 5 rows from v1 to v2; after ~9 runs the entire 45-chunk corpus is at v2 and `stale_served` drops to 0.

Empirical 2026-05-19 local run with a 2-day-old staging-cloned Aurora confirmed all of the above: 53,947 v1 + 2,500 v2 rows cluster-wide → first iteration on the small `common_budget_knowledge` context produced the exact summary line above.
