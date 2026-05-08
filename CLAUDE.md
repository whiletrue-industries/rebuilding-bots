# rebuilding-bots — Operational Notes for Claude

> Repo-specific notes that supplement `README.md`. The README covers
> architecture and end-user CLI usage; this file documents semantics
> and gotchas that bite during real debugging / deploy sessions.

## `botnim sync` semantics (post-2026-05-06 delta-sync)

Three explicit modes control what happens to the `documents` table:

| Invocation | Documents table | Cost |
|---|---|---|
| `botnim sync <env> <bot>` (default) | Delta — embed new/changed chunks; reuse unchanged via content-hash skip | Near-zero in steady state |
| `botnim sync <env> <bot> --force-rebuild` | DELETE all + re-embed every chunk | Full embed cost |
| `botnim sync <env> <bot> --replace-context none` | No-op for documents (still refreshes prompt + writes snapshot) | $0 |

Single-context targeting still works: pass `--replace-context legal_advisor_opinions` (or any other context slug) to process only that context. Combine with `--force-rebuild` to scope the wipe to that context only.

**Orphan handling:** delta mode keeps rows whose `source_id` no longer appears upstream — they linger in `documents` until an operator runs `--force-rebuild` to purge them. This is by design (resilient to upstream blips). For periodic cleanup, schedule a `--force-rebuild` per quarter or similar cadence.

**The previous default** (`replace_context=False` = no-op for documents) was a footgun — it made `botnim sync` after a fap silently throw away the new content. That default was changed on 2026-05-06.

**Extraction cache (post-2026-05-07):** Per-file `gpt-4o-mini` calls go through an Aurora-backed read-through cache (`extraction_cache` table, key = `(content_hash, extractor_version)`). On a fresh env the cache is cold and sync may RPD-out partway through `knesset_protocols` (~13K rows vs ~10K daily limit) — that's normal: the run logs `EXTRACTION RPD HIT … RESUME: …` and exits 0. Re-running the next day reads everything that was already extracted from the cache and pays the LLM cost only for the remaining unextracted files. After the cache warms, every subsequent sync costs ~0 LLM calls in steady state.

Bumping `EXTRACTION_VERSION` (in `botnim/dynamic_extraction.py`) is the right move when the prompt, model, or output schema changes — it invalidates all old payloads and forces re-extraction on the next sync. Use `--force-rebuild --replace-context <slug>` to purge the cache for one specific context (and re-derive both extractions and embeddings).

There is no `--no-extraction-cache` CLI flag in v1; the two escape hatches above cover the realistic operator workflows.

## Tool description overrides (post-2026-05-07 unified prompt editor)

Tool descriptions for context-search tools (auto-generated from `specs/<bot>/config.yaml`'s `name:` field) and OpenAPI tools (loaded from `specs/openapi/<tool>.yaml`'s `description:` field) can be overridden per-bot via the Aurora `agent_tool_overrides` table. Overrides are loaded by `bot_config.load_bot_config()` on every sync via `botnim/db/tool_overrides.py:get_active_tool_overrides(bot_slug)`, so a republish of the OpenAI Assistant picks up the latest active override without any spec-file edit.

**Inspect:**

```sql
SELECT tool_name, description, active, is_draft, created_at
FROM agent_tool_overrides
WHERE agent_type = 'unified'
ORDER BY created_at DESC LIMIT 20;
```

**Clear an override:**

- UI path (preferred): `/d/agent-prompts/<bot>` → ToolOverridesTable → row → "Clear override". Demotes the active row (preserves history; the previous active becomes inactive). Restorable via the version modal.
- SQL path (emergency only): `UPDATE agent_tool_overrides SET active = false WHERE agent_type = '<bot>' AND tool_name = '<TOOL>' AND active = true;`. The next call to `load_bot_config` will fall back to the canonical default from `config.yaml` / `specs/openapi/`.

**Cache key:** at most one row per `(agent_type, tool_name)` is active. Drafts are gated by `is_draft = true, active = false` and never reach `load_bot_config`; they only feed the DRAFT mirror agent.

**Migration:** alembic `0009_unified_prompt_editor` (creates `agent_tool_overrides` and the `agent_prompt_snapshots` view over the section history). Applied via `parlibot/deploy.sh` phase 6.5 — no manual `alembic upgrade` needed in the steady-state flow.
