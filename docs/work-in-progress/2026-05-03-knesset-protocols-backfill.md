# Knesset Protocols — Full Backfill (Work in Progress)

**Status as of 2026-05-03:** basic pipeline merged in PR #101 (commit
`de05725`). Configured for last 30 days × 100 protocols on staging
deploy. Full historical backfill (~$100, ~2-3 days, plenum-only) NOT yet
started.

This doc captures the measurements and the remaining implementation
work so the next session can pick up cleanly.

## Measurements (n=30 plenum docs sampled 2026-05-03)

| Metric | Value |
|---|---|
| Turns/doc — mean | 1,137 |
| Turns/doc — median | 1,175 |
| Turns/doc — p10 / p90 | 95 / 1,778 |
| Turns/doc — min / max | 32 / 2,459 |
| Tokens/turn — mean | 216 |
| Tokens/turn — median | 40 |
| Tokens/turn — p10 / p90 / max | 9 / 452 / **26,183** |

Source script: `/tmp/measure_plenum.py` (lives outside the repo;
reproduce by re-sampling 30 most-recent plenum docs and running
`parse_protocol` + `tiktoken.cl100k_base`).

## Cost projection (plenum-only, full ~20,510-doc corpus)

| Path | Embedding | Enrichment | Wall time |
|---|---|---|---|
| **Per-turn + enrichment ON** | $101 | $2,855 | ~83 days at SYNC_CONCURRENCY=8 (infeasible) |
| **Per-turn + enrichment OFF** | $101 | $0 | ~2-3 days |

Embedding: `text-embedding-3-small` @ $0.02/1M tokens × 5.0B tokens = $101.
Enrichment: `gpt-4o-mini` ($0.15 in / $0.60 out) × 23.3M chunks × ~150 out tokens.

User-approved path: **enrichment OFF**. Decision logged in Monday item
2814164913 thread.

## Open implementation TODOs

### 1. Add `skip_dynamic_extraction` per-context flag

The sync pipeline runs `extract_structured_content_async` (gpt-4o-mini)
on every chunk. For protocols that's 23M LLM calls and the parser
already produces all the structured metadata that enrichment would
synthesize. Add an opt-out so a context can declare itself
"already-structured" and skip the call.

* `botnim/collect_sources.py` — `_get_metadata_for_content_async`
  currently always runs extraction. Plumb a `skip_dynamic_extraction`
  flag from context config through to that function.
* `specs/unified/config.yaml` — set `skip_dynamic_extraction: true`
  on `knesset_protocols`.

### 2. Long-turn truncation in `process_protocols.py`

The sample shows max=26,183 tokens for one turn — exceeds
`text-embedding-3-small`'s 8,192 cap. Right now sync would crash on
those rows. Truncate to ~7,500 tokens (with tiktoken) before writing
to CSV; log when truncation fires so we can confirm it's rare.

Location: `botnim/document_parser/knesset_protocols/process_protocols.py:_process_doc`.

### 3. Switch context to plenum-only, larger window

In `specs/unified/config.yaml` for `knesset_protocols`:

```yaml
fetcher:
  kind: knesset_protocols
  base_url: https://knesset.gov.il/Odata/ParliamentInfo.svc
  days_history: 9999          # full history
  max_protocols: 25000        # > 20,510 plenum total = no cap
  rate_limit_seconds: 0.25
  include_committees: false
  include_plenum: true
```

(Committee corpus is ~140K docs, ~7× plenum, ~$700 just in embeddings —
deferred until plenum is proven.)

### 4. Two-step backfill on staging task

After deploying the above + ensuring `skip_dynamic_extraction` works:

```bash
# Inside botnim_api staging task, detached so SSM session lifetime
# doesn't bound it. Use the same setsid+nohup pattern that deploy.sh
# phase 8b uses for sync.
TASK=$(aws ecs list-tasks --profile anubanu-staging \
  --cluster buildup-staging --service-name botnim-api-staging-api \
  --desired-status RUNNING --query 'taskArns[0]' --output text)

# 1. Fetch (will take days at ~0.25s rate limit × 20K docs = ~1.5 hours
#    just downloads + ~2-3 hours parsing)
aws ecs execute-command --cluster buildup-staging --task "$TASK" \
  --container api --interactive --command "sh -c 'rm -f /tmp/protocols-fetch.log /tmp/protocols-fetch.exit; setsid nohup sh -c \"AIRTABLE_API_KEY=dummy botnim fetch-and-process unified knesset_protocols knesset_protocols --environment staging; echo \\\$? > /tmp/protocols-fetch.exit\" > /tmp/protocols-fetch.log 2>&1 < /dev/null & sleep 2; echo started'"

# 2. Sync (this is the multi-day step — ~5B tokens to embed)
#    Use --replace-context knesset_protocols so partial state is rebuilt clean.
aws ecs execute-command --cluster buildup-staging --task "$TASK" \
  --container api --interactive --command "sh -c '... botnim sync staging unified --backend aurora --replace-context knesset_protocols ...'"
```

Watch points:
- pgvector HNSW index build on 23M rows — likely needs `lists` tuning
  or HNSW `m` / `ef_construction` revisit. The 0007 migration's HNSW
  config was sized for thousands, not tens of millions.
- Aurora storage: ~210 GB. Confirm the cluster has headroom before
  starting.
- OpenAI rate-limit hits — embedding-3-small tier-3 is 5M tokens/min;
  with 5B total, ideal floor is ~17 hours. Real wall time more like
  2-3 days with backoff.

### 5. DoD validation after backfill

* Performance test: search latency on full corpus. Target <2s p95.
* End-to-end UI test: ask a specific protocol question through
  LibreChat staging, expect tool call + cited turn excerpt with
  `file_url` + speaker.
* Update Monday item 2814164913 with proof + run PROVE_DOD --force.

## Branch state

This branch (`wip/knesset-protocols-full-backfill`) only contains
this doc. The next session should:

1. Read this doc.
2. Create a new branch off `origin/main`, e.g. `feat/knesset-protocols-backfill`.
3. Implement TODOs 1+2 (the code changes for skip_dynamic_extraction
   + truncation) on that branch, open PR + merge.
4. Update the spec (TODO 3), open separate PR + merge.
5. Run the backfill (TODO 4) via ECS Exec.
6. Validate + close DoD (TODO 5).
