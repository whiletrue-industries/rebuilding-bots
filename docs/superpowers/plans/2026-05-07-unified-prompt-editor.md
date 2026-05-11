# Unified Prompt Editor — Implementation Plan

Linked spec: `docs/superpowers/specs/2026-05-07-unified-prompt-editor-design.md`
Tasks: 15. Each task is bite-sized (≤ 1 working day) and TDD-shaped:
write the failing test first, then the production change, then verify.

> **Repos involved:**
> * `rebuilding-bots` (this repo) — alembic migration + bot_config hook + CLAUDE.md update
> * `LibreChat` — aurora.js extensions, controller routes, parser, draft Agent plumbing, UI, l10n, seed script, CLAUDE.md update
> * `parlibot` — CLAUDE.md update + deploy verification

---

## Task 1 — Worktree setup (rb + lc)

**Goal:** Two isolated worktrees so this work doesn't disturb live branches.

**Steps:**
1. From `parlibot/rebuilding-bots/`: `git worktree add .worktrees/unified-prompt-editor -b unified-prompt-editor origin/main`.
2. From `parlibot/LibreChat/`: `git worktree add .worktrees/unified-prompt-editor -b unified-prompt-editor origin/main`.
3. Confirm both branches track `origin/main`.

**Verification:** `git -C .worktrees/unified-prompt-editor status` clean in both repos.

---

## Task 2 — Schema migrations (alembic 0009)

**Goal:** Add `agent_tool_overrides` table + `agent_prompt_snapshots` view (spec §5.1).

**TDD steps:**
1. Write `botnim/db/migrations/versions/test_0009_unified_prompt_editor.py` (or extend the existing migration test harness): apply migration on a fresh test postgres, assert table exists, partial unique index exists, view returns expected groupings on synthetic `agent_prompts` rows.
2. Author `botnim/db/migrations/versions/0009_unified_prompt_editor.py` with `down_revision = "0008_extraction_cache"`. Use the DDL from spec §5.1.1 / §5.1.2 verbatim.
3. Add a downgrade that drops the view first then the table.

**Verification:** `pytest botnim/db/migrations/versions/test_0009_unified_prompt_editor.py` green; `alembic upgrade head && alembic downgrade -1 && alembic upgrade head` round-trips on the test DB.

---

## Task 3 — rebuilding-bots: tool description override hook

**Goal:** Make `_search_tool_for_context` and `openapi_to_tools` consult `agent_tool_overrides` (spec §5.2 Python side).

**TDD steps:**
1. Add `botnim/db/tool_overrides.py` with one function: `get_active_tool_overrides(bot_slug: str) -> dict[str, str]` returning `{tool_name: description}` for active rows. Cache for the lifetime of one sync invocation.
2. Write `tests/test_tool_overrides.py`: with two seeded rows (`search_unified__legal_text`, `fetchWordDocument`), `get_active_tool_overrides("unified")` returns both.
3. Modify `_search_tool_for_context` (`botnim/bot_config.py:139`) to accept an optional `overrides: dict[str, str] | None`; if the override key matches the constructed `name`, replace `description`. Default `None` for callers that don't have overrides loaded.
4. Modify `openapi_to_tools` (`botnim/bot_config.py:92`) similarly: signature gains `overrides: dict[str, str] | None = None`, replace description by `operationId` lookup.
5. Update the call sites inside `bot_config.load_bot_config` (around line 251) to load overrides once and pass them through.

**Verification:** `pytest tests/test_tool_overrides.py` green; `pytest tests/test_bot_config.py` green; manual `botnim sync local unified --backend aurora` against a local DB with one seeded override flips the description in the resulting Assistants update payload (verify with `--dry-run` if available, otherwise check the OpenAI API call args via test double).

---

## Task 4 — LibreChat aurora.js: tool override CRUD

**Goal:** Mirror the existing prompt section CRUD for tool overrides (spec §5.2 LibreChat side).

**TDD steps:**
1. Write `LibreChat/api/server/services/AdminPrompts/aurora.tool-overrides.test.js`. Use the existing test harness pattern (real PG via testcontainers if available, else mongodb-memory-server style for postgres). Seed migration 0009 then exercise CRUD.
2. Add to `LibreChat/api/server/services/AdminPrompts/aurora.js`:
   * `listToolOverrides(agentType)` — joins canonical tool list (from `bot_config` API call to rebuilding-bots) with active override rows; returns `{toolName, defaultDescription, override: {id, description, publishedAt} | null}[]`.
   * `listToolOverrideVersions({agentType, toolName})` — newest first.
   * `saveToolOverrideDraft({agentType, toolName, description, changeNote, createdBy})` — same shape as `saveDraft`.
   * `publishToolOverride({agentType, toolName, draftId, parentVersionId})` — flips active flag identically to `publish` (lines 84-115 of existing aurora.js).
   * `restoreToolOverride({agentType, toolName, versionId})` — mirrors existing `restore` (line 117).
3. Update `module.exports` (line 237).

**Verification:** `cd LibreChat/api && npx jest aurora.tool-overrides` green.

---

## Task 5 — LibreChat: unified-assemble parser (split-on-save)

**Goal:** Parse a joined prompt back into per-section bodies, the inverse of `assemble.ts` (spec §4.1).

**TDD steps:**
1. Write `LibreChat/packages/api/src/admin/prompts/parse.test.ts`. Round-trip: `parse(assemble(sections))` returns the same `(sectionKey, body)[]`, in the same order.
2. Add tests for the error cases: unknown section_key marker, missing marker before first body, duplicate section_key, body contains a literal `<!-- SECTION_KEY:` substring inside a code block (must be detected as malformed if it falls between markers — the parser cares about line-anchored markers only).
3. Implement `LibreChat/packages/api/src/admin/prompts/parse.ts`. Regex against line starts: `/^<!-- SECTION_KEY: ([^>]+) -->$/m`. Validate every parsed key is in the known section list (passed in as argument from caller).
4. Export from `LibreChat/packages/api/src/admin/prompts/index.ts`.
5. Re-build `packages/api`: `cd LibreChat && npm run build:data-provider && npx turbo run build --filter=@librechat/api`.

**Verification:** `cd LibreChat/packages/api && npx jest parse` green.

---

## Task 6 — LibreChat controller: draft routes

**Goal:** Add the new HTTP endpoints from spec §5.2 (LibreChat side).

**TDD steps:**
1. Find the existing prompts router (likely `LibreChat/api/server/routes/admin/prompts.js` or similar). Write a route-level test that boots Express with the router mounted and exercises GET `/joined` against a fixture DB.
2. Add the joined-prompt routes:
   * GET `/api/admin/prompts/:bot/joined` → calls `listSections(bot)`, returns `assemble(sections)` plus version IDs.
   * POST `/api/admin/prompts/:bot/joined/draft` → calls `parse(body.joinedText)`, then for each section calls `saveDraft` (existing). All-or-nothing: if any save fails, transaction rolls back.
   * POST `/api/admin/prompts/:bot/joined/publish` → enumerates all draft sections for this bot and publishes them in one transaction. Reuses existing `publish`.
3. Add the snapshot routes:
   * GET `/api/admin/prompts/:bot/snapshots` → `SELECT * FROM agent_prompt_snapshots WHERE agent_type = $1 ORDER BY snapshot_minute DESC LIMIT 200`.
   * POST `/api/admin/prompts/:bot/snapshots/:minute/restore` → for each `id` in `section_version_ids`, call existing `restore` inside one TX.
4. Add the tool override routes (using Task 4 service methods).
5. All routes gated by the existing admin role middleware.

**Verification:** `cd LibreChat/api && npx jest admin/prompts` green; manual smoke via `curl` against a local LibreChat instance returns 200 + correct payloads.

---

## Task 7 — LibreChat: draft Agent mirror

**Goal:** A live `<bot> — DRAFT` Agent doc in Mongo, kept in sync with the in-flight draft prompt + tool overrides (spec §5.4).

**TDD steps:**
1. Add `LibreChat/api/server/services/AdminPrompts/draftAgent.js` exporting `ensureDraftAgent({bot, instructions, tools})`. Looks up `Agent({name: canonicalName + " — DRAFT"})`, upserts with `draft: true`, points at same OpenAI Assistant ID, returns `_id`.
2. Hook into the joined-draft and tool-draft save paths added in Task 6: after a successful save, recompute the would-be-joined draft (using whatever rows exist with `is_draft=true`, falling back to `active=true` for sections that aren't in draft) and call `ensureDraftAgent`.
3. Test with mongodb-memory-server: save a draft, assert exactly one Agent doc with `draft: true`, instructions match the joined-draft string, tools contain the overridden description for any tool with an active override OR an active draft override (drafts win for the draft chat).

**Verification:** unit test green; manual verification by hitting POST `/joined/draft` and observing the Mongo Agent collection.

---

## Task 8 — LibreChat seed script: maintain draft Agent

**Goal:** `seed-botnim-agent.js` (already invoked by `deploy.sh` phase 9) also seeds the draft Agent so a fresh stack has both records (spec §5.4).

**TDD steps:**
1. Read current `LibreChat/scripts/seed-botnim-agent.js` to understand the canonical-agent upsert shape.
2. Add a parallel upsert: same OpenAI Assistant ID, name suffixed with " — DRAFT", `draft: true`, instructions = current published joined prompt (read from rebuilding-bots API or directly from Aurora using existing connection helper).
3. Re-run `node LibreChat/scripts/seed-botnim-agent.js` against a local Mongo + Aurora; verify two Agent docs exist for `unified`.

**Verification:** local run idempotent (running twice doesn't create a third doc); both docs share the same OpenAI Assistant ID; only the DRAFT one has `draft: true`.

---

## Task 9 — LibreChat middleware: `restrictDraftAgent`

**Goal:** Block any non-admin from selecting the draft agent (spec §5.4).

**TDD steps:**
1. Write `LibreChat/api/server/middleware/restrictDraftAgent.test.js`: with mocked req where `req.user.role !== 'admin'` and resolved agent has `draft: true`, middleware returns 403; admin role passes through.
2. Add `LibreChat/api/server/middleware/restrictDraftAgent.js`. Resolution order: must run *after* agent resolution (so we know `agent.draft`), *before* model invocation.
3. Register the middleware in the chat / endpoints router pipeline. Surgical: only the routes that select an agent need it (chat creation, message send).

**Verification:** Jest green; manual: in the LibreChat UI as a non-admin, paste `?agent_id=<draftId>` into the URL → 403 from the API. As admin, same URL works.

---

## Task 10 — LibreChat UI: rewrite `/admin/prompts/<bot>`

**Goal:** Replace the per-section list with one big textarea + version sidebar (spec §5.3).

**TDD steps:**
1. In `LibreChat/client/src/components/Admin/Prompts/` (or whatever the existing path is), find the current per-section component. Add a snapshot test of the new component (`<UnifiedPromptEditor />`) that renders the textarea pre-populated from a fixture `/joined` response.
2. Build the component:
   * `<textarea>` (monaco-editor or simple textarea — start simple) with the joined text.
   * Buttons: "Save draft", "Publish", "Try draft" (disabled until a draft exists), "Snapshots…".
   * Snapshots panel: vertical list of `agent_prompt_snapshots` rows with timestamp + "Restore" button. Restore opens a confirm modal showing the diff (use `react-diff-viewer` or similar; if not present, plain monospace before/after for v1).
3. React-Query hooks under `client/src/data-provider/AdminPrompts/queries.ts` mirroring existing patterns; query keys + mutation keys in `packages/data-provider/src/keys.ts`.
4. Wire "Try draft" to `window.open(\`/c/new?agent_id=${draftAgentId}\`)`.

**Verification:** Jest snapshot green; manual: load `/admin/prompts/unified` in dev, edit and save, verify draft Agent updates in Mongo, click "Try draft" and confirm the conversation uses the new prompt.

---

## Task 11 — LibreChat UI: tool description editor + draft chat link

**Goal:** Tools section of the same admin page (spec §5.3 Tools table).

**TDD steps:**
1. Snapshot test `<ToolOverridesTable />` rendering canonical tools + override status.
2. Build the table: rows = tools from GET `/tools`, expanded row = inline textarea for description + Save draft / Publish / Restore buttons + version history modal.
3. Make sure both prompt-draft and tool-draft saves trigger the same `ensureDraftAgent` plumbing from Task 7 (they do, by sharing the route handlers from Task 6).
4. Restore a historical override = call `restoreToolOverride`. "Clear override" button = same call with `versionId: null` (server-side: insert a new row with `description = canonical default` and `active=true`, OR delete-the-active-row semantics — pick one in Task 4 and stick with it).

**Verification:** Jest snapshots green; manual: edit a tool description, save draft, open Try draft, ask the bot a question that exercises that tool — the LLM should see the overridden description.

---

## Task 12 — l10n: add new UI strings (English-only required)

**Goal:** All new admin UI text goes through `useLocalize()` (LibreChat CLAUDE.md rule).

**Steps:**
1. Add new keys to `LibreChat/client/src/locales/en/translation.json` only:
   * `com_admin_prompt_editor_title`
   * `com_admin_prompt_editor_save_draft`
   * `com_admin_prompt_editor_publish`
   * `com_admin_prompt_editor_try_draft`
   * `com_admin_prompt_editor_snapshots`
   * `com_admin_prompt_editor_restore_confirm`
   * `com_admin_tool_override_default`
   * `com_admin_tool_override_clear`
   * `com_admin_tool_override_history`
2. Reference them via `useLocalize()` in the new components — no hard-coded strings.
3. Other languages will be auto-translated externally (per LibreChat CLAUDE.md).

**Verification:** grep for hard-coded English strings in the new components returns nothing meaningful; `npm run lint` clean.

---

## Task 13 — Tests (server + UI)

**Goal:** Round out the test suite added incrementally in Tasks 2-11. Add the integration + e2e gaps.

**Steps:**
1. **Server integration** (LibreChat/api jest):
   * Save draft → publish → next GET `/joined` returns the published text.
   * Save draft → restore via snapshot → published text matches the snapshot's section bodies.
   * Save tool override draft → publish → GET `/tools` reflects the new active description.
2. **rebuilding-bots integration** (pytest): with one active tool override row, run `bot_config.load_bot_config("unified")` and verify the resulting tools list contains the override description, not the YAML default.
3. **e2e (Playwright if available, else manual):**
   * Admin opens `/admin/prompts/unified`, edits prompt, saves draft, clicks Try draft, sends a message, gets a response that quotes the new instruction.
   * Non-admin can't reach the draft agent via deep link.

**Verification:** all suites green; manual scripts captured under `docs/superpowers/manual-tests/2026-05-07-unified-prompt-editor.md` for repeatability.

---

## Task 14 — CLAUDE.md updates (rebuilding-bots + LibreChat)

**Goal:** Document the new flow + escape hatches so future operators don't regress.

**Steps:**
1. `rebuilding-bots/CLAUDE.md` — add a "Tool description overrides" subsection under the existing prompt-edit notes: explain that overrides are loaded by `bot_config` on every sync, where the table lives, how to inspect via psql, and how to clear an override (delete the active row OR republish the canonical YAML default through the UI).
2. `LibreChat/CLAUDE.md` — add a "Unified Prompt Editor" section: where the routes live, how the draft Agent works, the `restrictDraftAgent` middleware, and the seed-script extension for fresh stacks.
3. `parlibot/CLAUDE.md` — add a sanity-check entry under "How to deploy to staging" that mentions phase 8c also picks up new `agent_tool_overrides` migrations and that phase 9 now seeds two agents.

**Verification:** read each updated doc top-to-bottom, no stale references.

---

## Task 15 — PR / merge / deploy / verify

**Goal:** Ship to staging, run gold-set, then ship to prod.

**Steps:**
1. PRs:
   * `rebuilding-bots#unified-prompt-editor` — alembic 0009 + bot_config hook + CLAUDE.md.
   * `LibreChat#unified-prompt-editor` — aurora.js + parser + controller + draft Agent + middleware + UI + seed + l10n + CLAUDE.md.
   * `parlibot#unified-prompt-editor` — CLAUDE.md only.
2. Run compose-test gate locally (per the `feedback_skip_tests` memory): `docker compose -f compose.test.yml up --abort-on-container-exit`. **Do not pass `--skip-tests` to deploy.sh.**
3. Merge in order: rebuilding-bots → LibreChat → parlibot. Wait for `Build Docker Images` workflow (rebuilding-bots) and the LibreChat image build to finish.
4. Run `./deploy.sh staging --auto-approve`. Watch phase 8 (sync) and phase 9 (seed) logs — phase 9 should now upsert both the canonical and the draft Agent.
5. Open `https://botnim.staging.build-up.team/admin/prompts/unified`, smoke-test edit → save draft → try draft → publish → snapshot → rollback.
6. Run gold-set against staging (`deploy/gold-set.json`); verify all checks pass.
7. After staging soak (≥ 24h), repeat with `./deploy.sh prod`. Per the prod gotchas in `parlibot/CLAUDE.md`, phase 9 may need the manual `aws ecs run-task` recipe — apply it if the deploy script's exec-command path fails.

**Verification:** UptimeRobot (https://dashboard.uptimerobot.com/monitors/802831593) stays green through both deploys; gold-set pass rate unchanged or improved on each env.
