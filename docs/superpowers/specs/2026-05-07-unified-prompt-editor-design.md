# Unified Prompt Editor — Design

Status: design
Authors: prompt-editor working group
Created: 2026-05-07
Linked plan: `docs/superpowers/plans/2026-05-07-unified-prompt-editor.md`

## 1. Goal

Give the unified bot's product owner a single LibreChat admin page that lets them:

1. Edit the unified bot's whole system prompt as one continuous document
   (no per-section UX).
2. Edit per-tool descriptions for every tool the unified bot calls
   (`search_*` data tools and OpenAPI-defined tools), with the same
   draft / publish / rollback semantics the prompt sections already have.
3. Open a draft chat with the unified bot — same OpenAI Assistant ID,
   same retrieval — using the in-flight draft prompt + tool descriptions,
   without affecting any other user's session.
4. Browse version history and roll back the prompt and/or any tool
   description with a single click.

The bot administrator is non-technical. They must never need to edit
files in `specs/`, run `botnim sync`, or call an HTTP route by hand.

## 2. Why

Verbatim from the user request that triggered this work:

> "Give us, the bot owners, a real prompt editor for the unified bot.
> I want to edit the prompt and the tool definitions/descriptions in
> the LibreChat admin UI, hit save, then open a draft chat with that
> exact prompt and try things out before publishing. And I want a
> version list with rollback — the current setup forces me to ship a
> PR + redeploy + sync just to fix a sentence, which is absurd."

Today the `/admin/prompts/<bot>` page already lists per-section rows
(intro, common-knowledge, etc.), but:

* There are nine sections and the boundaries are an internal artifact
  (`SECTION_KEY` markers); the owner sees nine textareas where they
  conceptually have one prompt.
* Tool descriptions are not editable in the UI at all. They live in
  `specs/openapi/*.yaml` and `specs/<bot>/config.yaml#contexts[].description`,
  shipping requires a PR to `whiletrue-industries/rebuilding-bots`.
* There is no draft mode. Every save publishes to all live sessions.
* There is no whole-bot rollback; only per-section history exists.

## 3. Current State

### 3.1 Where prompts live today

* Per-section rows in Aurora `agent_prompts` (one active row per
  `(agent_type, section_key)`), plus inactive history rows. CRUD lives
  in `LibreChat/api/server/services/AdminPrompts/aurora.js` (lines 31,
  47, 59, 84, 117, 164, 176, 207).
* Sections are joined into a single instruction string at sync time by
  `LibreChat/packages/api/src/admin/prompts/assemble.ts` — which emits
  an `<!-- SECTION_KEY: <key> -->` marker before each body and joins
  bodies with `\n` (lines 8-21, exact format already in production).
* The Python sync side reads the joined string from Aurora via
  `botnim/bot_config.py:_load_instructions_from_aurora` (line 185) and
  pushes it to the OpenAI Assistant's `instructions` field.
* The shell tool that originally seeded the section rows is
  `rebuilding-bots/scripts/insert_prompt_section.py` — it is run once
  per `specs/<bot>/prompt_sections/*.md` file from `deploy.sh` phase 8c.

### 3.2 Where tool definitions live today

* `search_*` data tools are emitted by
  `botnim/bot_config.py:_search_tool_for_context` (line 139). The
  `description` comes from `context_cfg["description"]` (or `examples`
  appended) — i.e. straight out of `specs/<bot>/config.yaml`.
* OpenAPI-defined tools are converted by
  `botnim/bot_config.py:openapi_to_tools` (line 92). The `description`
  is `method["description"]` from the OpenAPI YAML files in
  `specs/openapi/*.yaml`.
* The complete tool list is then attached to the OpenAI Assistant on
  every `botnim sync`. Nothing about tools is currently in Aurora.

### 3.3 Where versioning currently exists (and doesn't)

* Per-section: `agent_prompts` has `active`, `is_draft`,
  `parent_version_id`, `change_note`, `created_by`, `published_at` —
  full history per row. `aurora.js` exposes `listVersions`,
  `saveDraft`, `publish`, `restore`.
* Whole-bot snapshot: does not exist. There is no way to ask "what
  was the entire prompt of `unified` at 2026-04-01T12:00Z?" without
  joining history rows by `published_at` ranges.
* Tools: no history at all. The current YAML descriptions are the only
  source.

## 4. Decisions

### 4.1 Section flattening — Option B (UI joins on read, server splits on save)

**Decision:** Keep `agent_prompts` rows section-shaped under the hood.
The admin UI presents one big textarea, populated by joining the
existing per-section bodies (with the same `<!-- SECTION_KEY: ... -->`
markers `assemble.ts` produces). On save, a parser on the LibreChat
backend splits the textarea by those markers and writes one draft row
per section.

**Why:** Three other code paths already depend on `agent_prompts`
being section-shaped:

* `aurora.js` history APIs are per-section (a flat `prompt` table
  would lose them).
* `insert_prompt_section.py` ships new sections from disk via a
  `(section_key, ordinal)` lookup; flattening would break the
  bootstrap path used by `deploy.sh` phase 8c.
* `assemble.ts` is also re-used by tooling outside this UI (sync
  comparisons, snapshot rendering).

Option B keeps all of that intact and is invisible to the owner.

### 4.2 Tool-edit v1 scope — description-only override

**Decision:** v1 lets the owner override only the `description` field
of any existing tool. The tool's `name` and `parameters` remain
canonical from `config.yaml` / `*.yaml`. Adding new tools, renaming
tools, or editing parameter schemas is explicitly out of scope (§8).

**Why:** Description is 95% of what the owner actually needs to tune
("teach the model when to call this tool"). Touching `name` or
`parameters` cross-cuts into Python tool-handler dispatch (`search_<index>`
→ index resolution) and invites silent prod breakage that is much harder
to roll back. v1 keeps the blast radius bounded to "the LLM's tool
selection heuristic".

### 4.3 Draft chat — D3 separate Agent record sharing OpenAI Assistant ID

**Decision:** Create a second LibreChat `Agent` document in MongoDB
named `<bot> — DRAFT` (e.g. `בוט מאוחד - תקנון, חוקים ותקציב — DRAFT`).
It points at the same OpenAI Assistant ID and the same retrieval
backend, but with a `draft: true` flag. The admin UI exposes a
"Try draft" button that opens
`/c/new?agent_id=<draftAgentId>&modelSpec=<draftSpec>` in a new tab.
A new server middleware (`restrictDraftAgent`) blocks any non-admin
user from selecting that agent.

**Why:** Three options were considered:

* D1 — header-driven override on the prod agent (e.g.
  `X-Botnim-Draft: 1`). Rejected: bleeds into every code path that
  caches by `(agentId)`, including assistant-side instruction caching,
  and is invisible to LibreChat's existing access controls.
* D2 — sandbox `Agent` per-admin. Rejected as overkill for v1; only
  one owner today.
* D3 — single shared `<bot> — DRAFT` agent (chosen). Same OpenAI
  Assistant ID = same retrieval, same model. The Agent record is just
  a LibreChat-side façade carrying overridden instructions and tools.
  Multi-admin can be added later by namespacing
  (`<bot> — DRAFT — <userId>`).

### 4.4 Versioning — reuse `agent_prompts` history, add `agent_prompt_snapshots` view

**Decision:** Per-section history is unchanged. To support whole-bot
rollback, add a database view `agent_prompt_snapshots` that returns
one row per `(agent_type, snapshot_minute)` where `snapshot_minute =
date_trunc('minute', published_at)`, aggregating the section IDs that
were active at that minute. The UI's "version list" is this view; a
"rollback" action re-publishes that exact set of section versions in
one transaction.

Granularity of one minute means two publishes inside the same minute
collapse into one snapshot row — acceptable for v1 (the owner is one
human; sub-minute consecutive publishes are accidental clicks, not
real intent).

**Why:** Snapshot tables would duplicate every published version's
text. A view is free, never drifts, and reuses the existing source of
truth.

### 4.5 No new versions table — pointer logic via existing `active=true` flag

**Decision:** `agent_prompts.active=true` (one row per
`(agent_type, section_key)`) remains the canonical "current published"
pointer. Drafts have `is_draft=true, active=false`. Publishing flips
the previously active row to `active=false` and the draft to
`active=true, is_draft=false, published_at=now()` (already the
behavior in `aurora.js:publish` lines 84-115). Rollback restores by
inserting a new row that copies a historical body, mirroring the
existing `restore` path (line 117).

**Why:** The current schema already encodes "current pointer" via
`active`. Adding a separate `current_version_id` column would create
two sources of truth that must agree. The existing model is correct;
the new UI just needs to use it.

## 5. Architecture

### 5.1 New tables

#### 5.1.1 `agent_tool_overrides`

Mirrors `agent_prompts` semantics one-for-one — same active uniqueness,
same draft + parent + restore handling, same RLS posture.

```sql
CREATE TABLE agent_tool_overrides (
  id                BIGSERIAL PRIMARY KEY,
  agent_type        TEXT NOT NULL,
  tool_name         TEXT NOT NULL,           -- e.g. 'search_unified__legal_text', 'fetchWordDocument'
  description       TEXT NOT NULL,
  active            BOOLEAN NOT NULL DEFAULT false,
  is_draft          BOOLEAN NOT NULL DEFAULT false,
  parent_version_id BIGINT REFERENCES agent_tool_overrides(id),
  change_note       TEXT,
  created_by        TEXT,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  published_at      TIMESTAMPTZ
);

CREATE UNIQUE INDEX agent_tool_overrides_active_uniq
  ON agent_tool_overrides (agent_type, tool_name)
  WHERE active = true;

CREATE INDEX agent_tool_overrides_lookup
  ON agent_tool_overrides (agent_type, tool_name, created_at DESC);
```

When no row with `active=true` exists for a given `(agent_type, tool_name)`,
the bot falls back to the canonical description from
`config.yaml` / OpenAPI YAML (i.e. unedited default).

#### 5.1.2 `agent_prompt_snapshots` view

```sql
CREATE VIEW agent_prompt_snapshots AS
SELECT
  agent_type,
  date_trunc('minute', published_at)               AS snapshot_minute,
  array_agg(id ORDER BY ordinal)                   AS section_version_ids,
  array_agg(section_key ORDER BY ordinal)          AS section_keys,
  max(created_by)                                  AS published_by
FROM agent_prompts
WHERE published_at IS NOT NULL
GROUP BY agent_type, date_trunc('minute', published_at);
```

The UI lists rows from this view (newest first) for whole-bot rollback.
"Rollback to snapshot X" is implemented as a server-side transaction
that, for each section in `section_version_ids`, calls the same
`restore` flow `aurora.js` already uses (line 117) — so the existing
per-section history continues to grow forward; rollback never deletes
history.

### 5.2 Server routes

#### LibreChat (`/api/admin/prompts/...`) — new in this work

| Method | Path | Purpose |
|---|---|---|
| GET | `/api/admin/prompts/:bot/joined` | Return `assemble(sections)` joined text + section ordinals + active version IDs |
| POST | `/api/admin/prompts/:bot/joined/draft` | Body = full joined text. Server splits by `<!-- SECTION_KEY: ... -->`, validates every section_key is known, writes one draft per section. Returns drafts + summary |
| POST | `/api/admin/prompts/:bot/joined/publish` | Publishes all drafts under this bot atomically; mirrors `aurora.js:publish` per section |
| GET  | `/api/admin/prompts/:bot/snapshots` | List rows from `agent_prompt_snapshots` (newest first) |
| POST | `/api/admin/prompts/:bot/snapshots/:minute/restore` | Restore all sections to that snapshot's version ids in one transaction |
| GET  | `/api/admin/prompts/:bot/tools` | List tools (canonical name+default desc + active override if any) |
| GET  | `/api/admin/prompts/:bot/tools/:toolName/versions` | History for one tool override |
| POST | `/api/admin/prompts/:bot/tools/:toolName/draft` | Save a description draft |
| POST | `/api/admin/prompts/:bot/tools/:toolName/publish` | Publish current draft |
| POST | `/api/admin/prompts/:bot/tools/:toolName/restore` | Restore a historical override (or `null` body to clear override → fall back to default) |

All gated by the existing admin role check on the prompts router.

#### rebuilding-bots (FastAPI / sync.py / bot_config.py)

The Python side does not gain new HTTP routes. Instead, two existing
helpers learn to read overrides:

* `_search_tool_for_context` (bot_config.py:139): after computing the
  default description, look up `agent_tool_overrides` for
  `(bot, "search_<index>")` — if active row exists, replace `description`.
* `openapi_to_tools` (bot_config.py:92): same hook keyed on the
  operation_id. (Function gains a `bot_slug` parameter so it can
  perform the lookup; default `None` keeps existing callers working
  with unedited defaults.)

Both helpers run inside `botnim sync`, so newly-published overrides
take effect on the next sync — exactly like prompt sections do today.

### 5.3 UI surface

A single new admin route under LibreChat: `/admin/prompts/:bot`.
Layout:

* **Top — "Prompt"** (full width)
  * One large textarea pre-populated with the joined assembly.
  * Section markers (`<!-- SECTION_KEY: ... -->`) render as visible
    sentinel lines (read-only styling — owner sees them but cannot
    accidentally delete one without intent).
  * Buttons: "Save draft", "Publish", "Try draft" (deep-link to the
    draft Agent), "Snapshots…" (opens snapshot list).
* **Below — "Tools"** (table)
  * Columns: Tool name | Default description | Active override (Y/N) |
    Edit | History
  * Edit opens an inline editor for the description; same Save
    draft / Publish / Restore actions.
* **Snapshots panel** — list of `agent_prompt_snapshots` rows. Each
  has a "Restore" button. Restoring opens a confirmation modal that
  shows a diff of the joined text vs the current published one.

### 5.4 Draft chat plumbing

* On any "Save draft" (prompt or tool override), the LibreChat backend
  calls `ensureDraftAgent({ bot })`. This:
  * Looks up the canonical Agent (`name = "<bot canonical name>"`).
  * Looks up or inserts a Mongo Agent doc with
    `name = "<canonical name> — DRAFT"`, `model_parameters` cloned
    from the canonical agent, `instructions` = joined draft text,
    `tools` = canonical tool list with overrides applied, `draft: true`.
  * Returns its `_id`.
* The "Try draft" button deep-links to `/c/new?agent_id=<draftId>`.
* `restrictDraftAgent` middleware (registered before agent
  resolution) returns `403` if the requesting user is not an admin
  AND the resolved agent has `draft: true`. Prevents accidental
  exposure of in-flight prompts to end users via shared links.
* `seed-botnim-agent.js` is extended (Task 8) to upsert the draft
  Agent record alongside the canonical one on every deploy, so a
  fresh Mongo restore (e.g. after the bootstrap recipe in
  `parlibot/CLAUDE.md`) ends up with both records.

## 6. Migration plan

1. Apply alembic `0009_unified_prompt_editor` (creates
   `agent_tool_overrides` and the `agent_prompt_snapshots` view). The
   existing 9 active section rows in `agent_prompts` are not touched.
2. `agent_tool_overrides` starts empty → all tool descriptions resolve
   to defaults (unchanged behavior).
3. `agent_prompt_snapshots` is computed dynamically, no backfill.
4. First deploy that includes Task 7 will create the `<bot> — DRAFT`
   Agent in Mongo via the extended seed; subsequent deploys are
   idempotent.
5. No data migration is needed for the joined-textarea read path —
   `assemble.ts` already produces the exact format the new GET
   `/joined` route returns.

## 7. Test plan

* **Unit (LibreChat backend, packages/api jest):**
  * `unified-assemble parser` round-trips: assemble(sections) →
    parse(joined) returns the same `(section_key, body, ordinal)`
    tuples in order.
  * Parser rejects unknown `section_key` markers, rejects empty
    bodies, rejects markers in the wrong order.
  * `aurora.js` new methods (`listToolOverrides`,
    `saveToolOverrideDraft`, `publishToolOverride`,
    `restoreToolOverride`) — full CRUD with mongodb-memory-server +
    Aurora-equivalent test postgres.
  * `agent_prompt_snapshots` view returns the right groupings (test
    fixture: publish 5 sections in two minute-buckets, view returns 2
    snapshots).
* **Unit (rebuilding-bots, pytest):**
  * `_search_tool_for_context` and `openapi_to_tools` apply override
    when active row exists, fall back to default otherwise.
* **Integration (LibreChat):**
  * Admin saves a draft → calls `ensureDraftAgent` → Mongo doc exists
    with `draft: true`.
  * Non-admin user attempts to use draft agent → 403.
  * Snapshot rollback restores all sections atomically (no partial
    state if one fails).
* **End-to-end (staging via deploy.sh):**
  * Open `/admin/prompts/unified`, edit prompt, save draft, click
    "Try draft" → new conversation answers with the new instructions.
  * Publish → next sync (deploy.sh phase 8b) updates the OpenAI
    Assistant's `instructions` to the published joined text.
  * Edit a tool description → publish → verify gold-set substring
    match on a query that exercises that tool.

## 8. Out of scope (v2)

* Per-user drafts (multiple admins editing in parallel without seeing
  each other's work). The shared `<bot> — DRAFT` agent makes this a
  "last writer wins" today — fine for one owner.
* Editing tool `name` or `parameters` schema. Requires changes to
  Python tool-handler dispatch and OpenAPI YAML reconciliation.
* Defining wholly new tools from the UI (no Python handler exists for
  a UI-defined tool name).
* Diff visualization beyond the "before snapshot vs current" view in
  the rollback modal — full inline diff per-section is a v2 polish.
* Cross-bot snapshots (e.g. "snapshot of unified + budget-explorer
  together"). Each bot is its own `agent_type` and rolls back
  independently.
* Auto-revert on bad metric (e.g. drop in gold-set pass rate after a
  publish). v1 expects the human owner to publish + watch + manually
  rollback if needed; auto-revert needs a metrics pipeline.
