# Sync contract for the LibreChat side of the Assistants -> Responses API migration

> Target branch: `feat/prompts-migration` on `rebuilding-bots`.
> Companion task: the LibreChat agent migrates its chat-time call from
> `client.beta.threads.runs.create(assistant_id=...)` to
> `client.responses.create(...)` using the JSON bundle described below.

## TL;DR

- **There are no `prompt_id`s.** OpenAI's "Prompts" (the official
  Assistants replacement) are dashboard-only; there is no SDK / REST
  surface to create or update them from a secret API key. I verified this
  against `openai==2.32.0` (no `client.prompts`), against
  `https://api.openai.com/v1/prompts` (returns 404 for secret keys), and
  against `/v1/dashboard/prompts` (returns 401 "must be made with a
  session key, i.e. from the browser"). OpenAI's own migration guide says
  "Prompts can only be created in the dashboard."
- **Replacement contract**: LibreChat fetches a JSON bundle from the
  botnim API (`GET /botnim/config/<bot>?environment=<env>`) and uses its
  fields directly in `client.responses.create(...)`. The bundle is
  rebuilt deterministically from `specs/<bot>/` on every call, so
  CI-synced spec changes are picked up without any extra deploy step.

## HTTP contract

### `GET /botnim/bots`

Returns the list of available bots.

```json
[
  {"slug": "unified", "name": "בוט מאוחד ...", "description": "..."}
]
```

### `GET /botnim/config/<bot>?environment=<env>`

`bot` is `unified`.
`environment` is one of `production`, `staging`, `local`; defaults to the
server-side default (`staging`).

Returns the full BotConfig bundle:

```json
{
  "slug": "unified",
  "name": "בוט מאוחד - תקנון, חוקים ותקציב - פיתוח",
  "description": "...",
  "environment": "staging",
  "model": "gpt-5.4-mini",
  "instructions": "You are an expert data researcher...",
  "temperature": 1e-05,
  "tools": [
    {
      "type": "function",
      "name": "search_unified__common_budget_knowledge__dev",
      "description": "ידע רלוונטי על התקציב",
      "parameters": {
        "type": "object",
        "properties": {
          "query":        {"type": "string", "description": "..."},
          "search_mode":  {"type": "string", "enum": [...], "default": "REGULAR"},
          "num_results":  {"type": "integer", "default": 7}
        },
        "required": ["query"]
      }
    },
    {"type": "code_interpreter"},
    {"type": "function", "name": "DatasetInfo",           "description": "...", "parameters": {...}},
    {"type": "function", "name": "DatasetFullTextSearch", "description": "...", "parameters": {...}},
    {"type": "function", "name": "DatasetDBQuery",        "description": "...", "parameters": {...}}
  ]
}
```

Error shapes:

- `404 {"detail": "Unknown bot 'nope'. Valid: [...]"}`
- `400 {"detail": "Invalid environment 'nope'. Valid: [...]"}`

### Tool shape

Tools use the **flat Responses API shape** (note: `name`, `description`,
`parameters` at the top level, no nested `"function": {...}` wrapper).
That is deliberate: `client.responses.create(tools=...)` takes this
shape. If you try to pass the old Assistants-API nested shape you'll get
400 errors.

## Chat-time flow LibreChat should adopt

Before (Assistants API):

```python
# one-time, read once from LIBRECHAT_BOTNIM_ASSISTANT_ID env:
run = client.beta.threads.runs.create_and_poll(
    thread_id=thread_id,
    assistant_id=ASSISTANT_ID,
    temperature=0,
)
```

After (Responses API):

```python
# per-request, or cache by (bot, environment) with a short TTL (e.g. 60s)
cfg = httpx.get(f"{BOTNIM_API}/botnim/config/{bot}?environment={env}").json()

response = client.responses.create(
    model=cfg["model"],
    instructions=cfg["instructions"],
    tools=cfg["tools"],
    temperature=cfg["temperature"],
    input=[{"role": "user", "content": user_message}],
    # conversation=conversation_id,  # if you're using Conversations; otherwise pass history yourself
)
```

The tool-execution loop on `function_call` items in `response.output`
stays the LibreChat agent's responsibility and mirrors the existing
Assistants-API `submit_tool_outputs` loop. The `function_call_output`
input item type is documented at
<https://platform.openai.com/docs/guides/function-calling>.

## Configuration / deploy changes

- **Drop `LIBRECHAT_BOTNIM_ASSISTANT_ID`**. Replace with `BOTNIM_BOT_SLUG`
  (`unified`) and `BOTNIM_ENVIRONMENT` (`staging` / `production`). The bot
  config is fetched from the botnim API; LibreChat holds no OpenAI IDs.
- `botnim sync` prints `Bot config published: <slug> (<env>) -> <path>`
  on success (replaces the old `Assistant updated: asst_...` line).
  `LibreChat/sync-*.sh` scripts that greppped for `Assistant updated`
  should look for `Bot config published` instead.

## Sync timing / caching

- `botnim sync <env> <bot> --backend es` now does two things:
  1. Refreshes Elasticsearch indices (unchanged semantics).
  2. Writes `specs/.published/<env>/<bot>.json` inside the botnim_api
     container. The FastAPI handler at `GET /config/<bot>` re-reads from
     `specs/<bot>/` on every request (not from the published file), so
     CI-synced spec changes are picked up by LibreChat on the next
     request without any restart.
- The `specs/.published/` artifact is produced deterministically and is
  intended for out-of-band consumers (e.g. an S3 publish step or a
  CDN). It is `.gitignore`d.
- There's no explicit cache invalidation needed. If LibreChat caches the
  config client-side, a 60-second TTL is fine.

## Dry-run proof (local Docker, 2026-04-16)

Run inside the `botnim_api` container:

```
$ docker exec -e AIRTABLE_API_KEY=dummy botnim_api botnim sync staging unified --backend es
Syncing unified to staging
INFO:botnim.sync:Syncing bot: unified (env=staging, backend=es)
...
INFO:botnim.bot_config:Published bot config: /srv/specs/.published/staging/unified.json
INFO:botnim.sync:Bot config published: slug=unified env=staging model=gpt-5.4-mini
 tools=11 instructions_chars=21511 path=/srv/specs/.published/staging/unified.json
Bot config published: unified (staging) -> /srv/specs/.published/staging/unified.json

$ docker exec botnim_api curl -s 'http://localhost:8000/config/unified?environment=staging' | jq 'keys'
[
  "description",
  "environment",
  "instructions",
  "model",
  "name",
  "slug",
  "temperature",
  "tools"
]
```

No `client.beta.assistants.*` calls in the sync path. No OpenAI
dashboard interaction required to deploy a prompt change.

## Definition of done (coordination-level)

1. LibreChat's chat-time code calls `GET /botnim/config/<bot>` and passes
   the returned fields into `client.responses.create(...)`. The old
   `client.beta.threads.runs.create(assistant_id=...)` is removed.
2. End-to-end Playwright test against local Docker answers both canonical
   questions with real content:
   - "מה תקציב משרד החינוך לשנת 2025?" cites the ~89.8B NIS figure.
   - "האם ניתן לתת לח״כ תרומה?" cites the ethics rules.

## Open questions / blockers

- None on the botnim side. If LibreChat hits any surprise (e.g. a tool
  shape the Responses API rejects), file it against
  `botnim/bot_config.py::openapi_to_tools` -- that is where the flat
  shape is emitted.
