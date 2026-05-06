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
