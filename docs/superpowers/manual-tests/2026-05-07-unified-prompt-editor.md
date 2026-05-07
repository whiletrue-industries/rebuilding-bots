# Unified Prompt Editor — Manual Test Script

Date: 2026-05-07
Spec: [2026-05-07-unified-prompt-editor-design.md](../specs/2026-05-07-unified-prompt-editor-design.md)
Plan: [2026-05-07-unified-prompt-editor.md](../plans/2026-05-07-unified-prompt-editor.md)

## Compose-test gate (run BEFORE staging deploy)

### Pre-requisites
- Local docker-compose stack running: `cd LibreChat && docker compose up -d`
- Postgres reachable at `localhost:54330` (per existing aurora.spec.js convention)
- Admin user logged in to LibreChat
- Non-admin user available for the auth check
- The seed script has run, so both `<bot>` and `<bot> — DRAFT` agents exist in Mongo

### Test 1: Unified editor round-trip

1. Open `/d/agent-prompts/unified`. Verify textarea pre-populated with assembled prompt + section markers (`<!-- SECTION_KEY: ... -->`).
2. Edit one section's body — add unique sentinel `TEST_SENTINEL_<random6>`.
3. Click "Save draft". Verify:
   - Visual draft indicator appears
   - "Try draft" button enables
   - Mongo `agents` collection has a doc with `name` ending in `— DRAFT` and `draft: true`; instructions contain the sentinel
4. Click "Try draft". New tab opens to `/c/new?agent_id=<draftId>`.
5. In the draft chat, ask a question that should hit the modified section. Verify response references the sentinel.
6. Back in admin, click "Publish". Refresh page. Verify textarea now shows the sentinel (active version).
7. Verify production chat (different tab, original `<bot>`, NOT the draft) — instructions also have the sentinel after publish (since publish flips active).

### Test 2: Snapshots + restore

1. Make 2-3 sequential publishes with distinct sentinels (`SENT_A`, `SENT_B`, `SENT_C`).
2. Open Snapshots panel. Verify ≥3 entries with sensible timestamps.
3. Click Restore on the FIRST snapshot (containing `SENT_A`).
4. Confirm dialog shows current sections + restoration target sections.
5. Confirm. Verify textarea reverts to `SENT_A`.
6. Verify Try draft chat reflects the restored prompt.

### Test 3: Tool description override

1. In ToolOverridesTable, expand row for `search_unified__legal_text__dev` (staging) or `search_unified__legal_text` (prod).
2. Edit description, add `TEST_TOOL_DESC_<random6>`.
3. Save draft. Open Try draft chat. Ask a question that uses that tool. Verify the override is in effect (e.g., bot's tool-call reasoning references the new description, or check the OpenAI Run logs).
4. Publish. Refresh table. Verify the override is now active. Default badge gone.
5. Click "Clear override". Verify table returns to canonical default. History still preserved (open Versions modal).
6. Restore from history. Verify table shows the override again.

### Test 4: Auth gate (`restrictDraftAgent`)

1. As admin: navigate to `/c/new?agent_id=<draftAgentId>`. Should work.
2. Open in private window / different browser, log in as non-admin user.
3. Try the same URL. Verify 403 from API (network tab) and UI shows error.
4. The non-admin user CAN still chat with the canonical (non-draft) bot.

## Compose verification table (fill before staging)

| Test | Result | Notes |
|---|---|---|
| 1: Round-trip | | |
| 2: Snapshots + restore | | |
| 3: Tool override | | |
| 4: Auth gate | | |

## Staging gate (after compose-test passes)

Same 4 tests against `https://botnim.staging.build-up.team`.

## Production gate (after staging passes)

Same 4 tests against `https://botnim.build-up.team`. Use sentinels that obviously look like tests (e.g., `TEST_SENTINEL_*`) so any leak is immediately spottable + easily rolled back via the snapshots restore.

## Rollback

If anything goes wrong post-publish:
1. Open `/d/agent-prompts/<bot>` in admin
2. Snapshots panel → Restore the most recent good snapshot
3. Verify production chat reflects the rollback within seconds (no LibreChat task restart needed)
