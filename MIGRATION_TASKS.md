# Migration Tasks: Assistants API → Responses API + ChatKit

## Task Dependency Graph

```
T0 (Baseline Capture) ─────────────────────────────────────────────────────┐
                                                                            │
T1 (Bot Config) ──┐                                                         │
                   ├──→ T3 (Response Loop) ──┬──→ T5 (CLI Update)           │
T2 (Tool Executor)─┘                         ├──→ T6 (Benchmark Update) ────┤ (compares to T0)
                                             ├──→ T7 (Backend Chat API)     │
T4 (Sync Update) ← T1                       │                              │
                                             ├──→ T8 (ChatKit Scaffold)     │
T10 (Observability) ← T3                    │    └→ T9 (ChatKit UX)        │
                                             │                              │
T11 (Test Suite) ← T1, T2, T3               ├──→ T12 (NGINX + Docker)      │
                                             │                              │
                                             ├──→ T14 (Deploy Tooling) ─────┤
                                             │                              │
                                             └──→ T13 (Cleanup) ← T14 ─────┘
```

**Parallelizable**: T0 + T1 + T2 (no deps). Then T4, T5, T6, T7, T10, T11 (after T3). Then T8, T9, T12, T14 (parallel). T13 is last (after T14).

---

## T0: Baseline Benchmark Capture

**Description**

Before any migration work begins, run the full benchmark suite against the current Assistants API implementation for both takanon and budgetkey bots. Save the results as baseline files. These baselines are the quality gate for T6 — the migrated system must score within 5% of these numbers. Also capture per-question tool call patterns (which tools called, in what order) to detect behavioral regressions after migration.

**Files to edit**

1. `out/baselines/TAKANON_QA_baseline.csv` (new — saved benchmark output)
2. `out/baselines/BUDGETKEY_QA_baseline.csv` (new — saved benchmark output)
3. `out/baselines/README.md` (new — documents when/how baselines were captured)

**Needed knowledge**

- How to run the benchmark suite: `botnim benchmarks staging takanon --local --select all` and same for `budgetkey`
- Output format: `out/benchmarks/TAKANON QA/` and `out/benchmarks/BUDGET QA/` contain datapackage.json + CSV with columns including `score`, `success`, `actual answer`, `notes`
- The `notes` column contains tool call traces (e.g. `search_unified__legal_text__dev({"query": ...})`) — save these for comparison

**Definition of done**

1. E2E: Baseline files exist and contain valid data
   ```bash
   python -c "
   import csv
   for bot in ['TAKANON_QA_baseline.csv', 'BUDGETKEY_QA_baseline.csv']:
       with open(f'out/baselines/{bot}') as f:
           reader = csv.DictReader(f)
           rows = list(reader)
           assert len(rows) > 5, f'{bot}: Expected at least 5 baseline questions'
           scores = [int(r['score']) for r in rows if r.get('score')]
           avg = sum(scores) / len(scores)
           print(f'{bot}: {len(rows)} questions, avg score: {avg:.1f}')
   print('PASS: Baselines captured')
   "
   ```
2. `out/baselines/README.md` records: date, environment, model version (`gpt-4.1`), assistant IDs used, git commit hash
3. Baseline files are committed to git so they survive across branches

---

## T1: Bot Config Module

**Description**

Create a new module `botnim/bot_config.py` that loads a bot's full configuration (system prompt, tools, model, temperature) from the local `specs/` directory into a dataclass consumable by the Responses API. This replaces the concept of a remote OpenAI "Assistant" object. Tool definitions must use the Responses API flat format (`{"type": "function", "name": ..., "parameters": ...}`) instead of the Assistants API nested format (`{"type": "function", "function": {"name": ..., "parameters": ...}}`).

Also update the tool generation in `vector_store_es.py` to produce the flat format.

**Files to edit**

1. `botnim/bot_config.py` (new)
2. `botnim/vector_store/vector_store_es.py` (modify `update_tools()` at line 520)
3. `botnim/vector_store/vector_store_base.py` (remove `tool_resources`, update return type of `vector_store_update()`)

**Needed knowledge**

- Responses API tool definition format: https://developers.openai.com/api/docs/guides/function-calling — tools are flat dicts with `type`, `name`, `description`, `parameters` at top level
- How `specs/*/config.yaml` and `specs/*/agent.txt` define each bot (read `specs/unified/config.yaml` and `specs/budgetkey/config.yaml` for examples)
- How `VectorStoreES.update_tools()` currently builds tool definitions (line 513-547 of `vector_store_es.py`)
- How `sync.py:openapi_to_tools()` converts OpenAPI specs to tool definitions (lines 11-46)
- The `is_production()` / `__dev` suffix convention in `config.py`

**Definition of done**

1. `from botnim.bot_config import load_bot_config` works without import errors
2. E2E validation script:
   ```python
   from botnim.bot_config import load_bot_config
   config = load_bot_config("unified", "staging")
   assert config.slug == "unified"
   assert len(config.instructions) > 100  # agent.txt loaded
   assert any(t["name"].startswith("search_unified__") for t in config.tools)
   assert any(t["name"] == "DatasetInfo" for t in config.tools)
   assert all("function" not in t for t in config.tools)  # NO nested format
   # Verify flat format: name is at top level, not under "function" key
   for t in config.tools:
       if t["type"] == "function":
           assert "name" in t
           assert "parameters" in t
   ```
3. `load_bot_config("budgetkey", "staging")` includes `code_interpreter` tool (`{"type": "code_interpreter"}`)
4. `load_bot_config("takanon", "production")` produces tool names WITHOUT `__dev` suffix

---

## T2: Tool Executor Module

**Description**

Extract the tool dispatch logic currently inline in `benchmark/assistant_loop.py` (lines 127-198) into a standalone module `botnim/tool_executor.py`. This module receives a tool name and arguments, routes to the appropriate handler (ES search or OpenAPI HTTP call), and returns a JSON-serialized string result. Includes the DatasetInfo caching logic currently at lines 34-45 of `assistant_loop.py`.

**Files to edit**

1. `botnim/tool_executor.py` (new)

**Needed knowledge**

- How `search_*` tools map to `run_query()` — see `assistant_loop.py` lines 137-159 and `botnim/query.py`
- How OpenAPI tools dispatch via `requests_openapi` — see `assistant_loop.py` lines 165-181 and the `get_openapi_output()` function at lines 19-32
- Search mode resolution: `SEARCH_MODES` dict and `DEFAULT_SEARCH_MODE` in `botnim/vector_store/search_modes.py`
- DatasetInfo YAML cache: `specs/budgetkey/dataset-info-cache/` directory

**Definition of done**

1. E2E: Search tool execution against live ES
   ```bash
   # From rebuilding-bots/ directory with .env loaded:
   python -c "
   from botnim.tool_executor import execute_tool
   result = execute_tool(
       'search_unified__common_takanon_knowledge__dev',
       {'query': 'ועדת אתיקה', 'search_mode': 'REGULAR'},
       environment='staging'
   )
   assert len(result) > 50, 'Expected non-empty search results'
   print('PASS: search tool returned', len(result), 'chars')
   "
   ```
2. E2E: OpenAPI tool execution against live budgetkey API
   ```bash
   python -c "
   from botnim.tool_executor import execute_tool
   result = execute_tool(
       'DatasetInfo',
       {'dataset': 'budget_items_data'},
       openapi_spec='budgetkey.yaml',
       environment='staging'
   )
   import json
   parsed = json.loads(result)
   assert 'fields' in str(parsed) or 'columns' in str(parsed), 'Expected schema info'
   print('PASS: DatasetInfo returned schema')
   "
   ```
3. Error handling: `execute_tool("unknown_tool", {})` returns a string containing "Error" rather than raising an exception

---

## T3: Response Loop

**Description**

Create `botnim/response_loop.py` — the new agentic loop replacing `benchmark/assistant_loop.py`. Uses `client.responses.create()` instead of threads/runs/polling. The loop: sends input items, receives output, checks for `function_call` items, executes tools via `tool_executor`, appends `function_call_output`, calls `responses.create()` again, repeats until no more tool calls. Returns a `ResponseLoopResult` dataclass with the final text, tool call log, token usage, and response ID.

**Files to edit**

1. `botnim/response_loop.py` (new)

**Needed knowledge**

- Responses API `client.responses.create()`: https://developers.openai.com/api/docs/guides/function-calling — specifically the Responses API tab examples
- Response output structure: `response.output` is a list of items, each with a `.type` field. `function_call` items have `.name`, `.arguments`, `.call_id`
- Function call output format: `{"type": "function_call_output", "call_id": ..., "output": ...}`
- The `instructions` parameter in `responses.create()` replaces the system message
- `response.output_text` gives the final text output
- Must import and use `BotConfig` from T1 and `execute_tool` from T2

**Definition of done**

1. E2E: Legal question answered correctly
   ```bash
   python -c "
   from openai import OpenAI
   from botnim.bot_config import load_bot_config
   from botnim.response_loop import response_loop
   from botnim.config import get_openai_client

   client = get_openai_client('staging')
   config = load_bot_config('takanon', 'staging')
   result = response_loop(client, config, 'מהו סעיף 106 לתקנון הכנסת?')

   assert len(result.output_text) > 100, 'Expected substantial answer'
   assert result.rounds >= 2, 'Expected at least 2 rounds (orientation + search)'
   assert any('search_' in tc.name for tc in result.tool_calls), 'Expected search tool calls'
   print('PASS: Legal question answered in', result.rounds, 'rounds')
   print('Tools called:', [tc.name for tc in result.tool_calls])
   "
   ```
2. E2E: Budget question with OpenAPI tools
   ```bash
   python -c "
   from openai import OpenAI
   from botnim.bot_config import load_bot_config
   from botnim.response_loop import response_loop
   from botnim.config import get_openai_client

   client = get_openai_client('staging')
   config = load_bot_config('budgetkey', 'staging')
   result = response_loop(
       client, config,
       'כמה תקציב קיבל משרד החינוך בשנת 2024?',
       openapi_spec='budgetkey.yaml'
   )

   assert len(result.output_text) > 100
   assert any('DatasetDBQuery' == tc.name for tc in result.tool_calls), 'Expected SQL query tool call'
   print('PASS: Budget question answered')
   "
   ```
3. Safety: Loop terminates within `max_rounds` (default 15) even if model keeps requesting tools

---

## T4: Update Sync Module

**Description**

Strip `sync.py` of all OpenAI Assistant object management. After this change, `botnim sync staging unified --backend es` only syncs Elasticsearch indices — it no longer calls `client.beta.assistants.create/update/list`. Remove the `openapi_to_tools()` function (moved to `bot_config.py` in T1). Change the default `--backend` CLI option from `openai` to `es` and remove the `openai` choice.

**Files to edit**

1. `botnim/sync.py` (remove ~60 lines)
2. `botnim/cli.py` (update `sync` command default and choices, line 28-33)

**Needed knowledge**

- Current `sync.py` structure: `update_assistant()` does both ES sync AND assistant CRUD. After this task, it should only do ES sync.
- `VectorStoreES.vector_store_update()` is the ES sync entry point (called from `update_assistant` at line 63)
- The `cli.py` sync command at line 24-33 currently defaults `--backend` to `openai`

**Definition of done**

1. E2E: Sync runs without creating/updating any Assistant objects
   ```bash
   botnim sync staging unified --backend es --replace-context all 2>&1 | tee /dev/stderr | \
     grep -v "beta.assistants" && echo "PASS: No assistant API calls"
   ```
2. E2E: ES indices are populated after sync
   ```bash
   python -c "
   from botnim.query import get_available_indexes
   indexes = get_available_indexes('staging')
   assert any('unified__legal_text' in idx for idx in indexes), 'Expected legal_text index'
   print('PASS: ES indexes present:', indexes)
   "
   ```
3. `botnim sync staging unified` (no `--backend` flag) defaults to `es` and succeeds
4. `--backend openai` choice is removed; running with it produces a CLI error

---

## T5: Update CLI Assistant

**Description**

Rewrite `cli_assistant.py` to use the new `response_loop` instead of the Assistants API. Replace assistant selection (by OpenAI assistant ID) with bot selection (by local slug from `specs/`). Update the `assistant` command in `cli.py` to accept `--bot` instead of `--assistant-id`. Since system prompts forbid cross-turn memory, each question in the conversation starts a fresh `response_loop` call.

**Files to edit**

1. `botnim/cli_assistant.py` (rewrite)
2. `botnim/cli.py` (update `assistant` command at line 140-153)

**Needed knowledge**

- Current `cli_assistant.py` flow: lists assistants via `client.beta.assistants.list()`, creates thread, loops user input → `assistant_loop()` → print response
- New flow: list bots from `specs/*/config.yaml`, load `BotConfig`, loop user input → `response_loop()` → print `result.output_text`
- The RTL handling logic (lines 60-67, 96-98) should be preserved
- `AVAILABLE_BOTS` from `config.py` provides the list of valid bot slugs

**Definition of done**

1. E2E: Interactive session can be started with `--bot` flag
   ```bash
   echo "מהו סעיף 106 לתקנון הכנסת?" | timeout 120 botnim assistant --bot takanon --environment staging
   # Should print a response containing "סעיף 106" or related content
   # Should NOT print any "beta.threads" or "beta.assistants" references in logs
   ```
2. E2E: Bot selection menu appears when no `--bot` provided
   ```bash
   echo "1" | timeout 10 botnim assistant --environment staging 2>&1 | head -20
   # Should show "Available Bots:" with numbered list including takanon, budgetkey, unified
   ```
3. Old `--assistant-id` flag is removed; using it produces a CLI error
4. `/stop` command still terminates the conversation

---

## T6: Update Benchmark Runner

**Description**

Update `benchmark/runner.py` to use `response_loop` and `BotConfig` instead of the Assistants API. Remove `client.beta.assistants.list()` lookups. The `fetch_single_answer()` function should call `response_loop()` directly and extract the answer from `result.output_text` instead of reading thread messages. Bot identification changes from assistant name lookup to local slug resolution.

**Files to edit**

1. `botnim/benchmark/runner.py` (modify)

**Needed knowledge**

- Current flow: `fetch_answer()` lists assistants by name (line 159), gets `assistant_id`, passes to `fetch_single_answer()` which calls `assistant_loop()` and reads messages from thread (lines 137-148)
- New flow: `fetch_answer()` calls `load_bot_config(bot_slug, environment)`, passes to `fetch_single_answer()` which calls `response_loop()` and uses `result.output_text`
- The benchmark uses `dataflows` for parallel processing (`DF.parallelize`). The new `response_loop` must be thread-safe (it is — each call creates independent state).
- Airtable integration and scoring logic remain unchanged
- Baseline scores from T0 are in `out/baselines/`

**Definition of done**

1. E2E: Run a single takanon benchmark question locally
   ```bash
   botnim benchmarks staging takanon --local --select <KNOWN_AIRTABLE_RECORD_ID> --concurrency 1
   # Should complete without error
   # Check out/benchmarks/TAKANON\ QA/ for result files
   # Verify "actual answer" field is non-empty in output
   ```
2. E2E: Run a single budgetkey benchmark question locally
   ```bash
   botnim benchmarks staging budgetkey --local --select <KNOWN_AIRTABLE_RECORD_ID> --concurrency 1
   # Should complete without error
   # Verify answer contains budget data (numbers, table formatting)
   ```
3. No imports from `openai.types.beta.threads` remain in `runner.py`
4. E2E: Quality gate against T0 baselines
   ```bash
   python -c "
   import csv
   for bot_file, baseline_file in [
       ('out/benchmarks/TAKANON QA/benchmark.csv', 'out/baselines/TAKANON_QA_baseline.csv'),
       ('out/benchmarks/BUDGET QA/benchmark.csv', 'out/baselines/BUDGETKEY_QA_baseline.csv'),
   ]:
       with open(baseline_file) as f:
           baseline_avg = sum(int(r['score']) for r in csv.DictReader(f) if r.get('score')) / max(1, sum(1 for _ in open(baseline_file)) - 1)
       with open(bot_file) as f:
           new_avg = sum(int(r['score']) for r in csv.DictReader(f) if r.get('score')) / max(1, sum(1 for _ in open(bot_file)) - 1)
       diff_pct = abs(new_avg - baseline_avg) / baseline_avg * 100
       assert diff_pct < 5, f'{bot_file}: Score regression {diff_pct:.1f}% (baseline={baseline_avg:.1f}, new={new_avg:.1f})'
       print(f'PASS: {bot_file} within {diff_pct:.1f}% of baseline')
   "
   ```

---

## T7: Backend Chat API

**Description**

Expand the existing FastAPI server (`backend/api/server.py`) with a `POST /chat` endpoint that receives a user question + bot slug, runs `response_loop` server-side, and returns the answer. Add a `POST /chat/stream` endpoint that returns SSE events for streaming responses. Add a `GET /bots` endpoint that lists available bots. All endpoints must be protected by Firebase authentication using the existing `FireBaseUser` dependency. These endpoints serve as the backend for the ChatKit frontend (T8/T9).

**Files to edit**

1. `backend/api/server.py` (add endpoints)
2. `botnim/response_loop.py` (add streaming support if not done in T3)
3. `backend/api/requirements.txt` (add `openai` dependency — currently only has `fastapi`, `uvicorn`, `firebase-admin`)

**Needed knowledge**

- Responses API streaming: `client.responses.create(stream=True)` returns an event stream with `response.output_text.delta` events — see https://developers.openai.com/api/docs/guides/streaming-responses
- FastAPI `StreamingResponse` for SSE: `from starlette.responses import StreamingResponse` with `media_type="text/event-stream"`
- Existing auth pattern: `FireBaseUser` dependency injection (see `backend/api/server.py` line 63 for usage in admin endpoints)
- The existing `GET /retrieve/{bot}/{context}` endpoint stays unchanged
- `backend/api/requirements.txt` currently lacks `openai` — the API container won't be able to import it without adding it

**Definition of done**

1. E2E: Non-streaming chat request
   ```bash
   curl -X POST http://localhost:8000/chat \
     -H "Content-Type: application/json" \
     -H "Authorization: Bearer <VALID_FIREBASE_TOKEN>" \
     -d '{"bot_slug": "takanon", "message": "מהו סעיף 106?", "environment": "staging"}' \
     | python -c "
   import sys, json
   resp = json.load(sys.stdin)
   assert 'response_text' in resp
   assert len(resp['response_text']) > 100
   assert 'tool_calls' in resp
   print('PASS: Chat endpoint returned', len(resp['response_text']), 'chars')
   "
   ```
2. E2E: Streaming chat request
   ```bash
   curl -N -X POST http://localhost:8000/chat/stream \
     -H "Content-Type: application/json" \
     -H "Authorization: Bearer <VALID_FIREBASE_TOKEN>" \
     -d '{"bot_slug": "takanon", "message": "מהו סעיף 106?", "environment": "staging"}' \
     --max-time 120 | head -20
   # Should receive SSE events: "data: {...}" lines with text deltas
   ```
3. E2E: Bot listing
   ```bash
   curl http://localhost:8000/bots | python -c "
   import sys, json
   bots = json.load(sys.stdin)
   slugs = [b['slug'] for b in bots]
   assert 'unified' in slugs
   assert 'takanon' in slugs
   assert 'budgetkey' in slugs
   print('PASS: Found', len(bots), 'bots')
   "
   ```
4. E2E: Auth enforcement
   ```bash
   # Request without auth token should return 401/403
   curl -s -o /dev/null -w "%{http_code}" -X POST http://localhost:8000/chat \
     -H "Content-Type: application/json" \
     -d '{"bot_slug": "takanon", "message": "test"}' | grep -q "40[13]" \
     && echo "PASS: Unauthenticated request rejected" || echo "FAIL"
   ```
5. Existing `GET /retrieve/{bot}/{context}` still works unchanged

---

## T8: ChatKit Frontend — Project Scaffold

**Description**

Create a new lightweight React frontend project for the ChatKit chat interface. The existing `ui/` directory is an Angular 18 admin app ("brbots") — do not modify it. Create a new `chatkit-ui/` project with React + Vite, install `@openai/chatkit-react`, and wire up the basic ChatKit component that connects to the backend API's session endpoint. This task is scaffolding only — no bot selector, no theming.

Note: The existing Angular app in `ui/` serves a different purpose (admin/management UI). The ChatKit frontend is the user-facing chat interface that replaces LibreChat.

**Files to edit**

1. `chatkit-ui/package.json` (new — React + Vite + ChatKit deps)
2. `chatkit-ui/src/main.tsx` (new — React entry point)
3. `chatkit-ui/src/App.tsx` (new — ChatKit widget mount with session fetch)
4. `chatkit-ui/index.html` (new — HTML shell)

**Needed knowledge**

- ChatKit React setup: https://developers.openai.com/api/docs/guides/chatkit — "Embed ChatKit in your frontend" section
- `@openai/chatkit-react` npm package: provides `<ChatKit />` component and `useChatKit()` hook
- ChatKit session flow: frontend calls `POST /chatkit/session` on your backend, receives `client_secret`, passes to `useChatKit({ api: { getClientSecret } })`
- ChatKit Advanced Integration: https://developers.openai.com/api/docs/guides/custom-chatkit — for connecting to your own backend instead of Agent Builder

**Definition of done**

1. E2E: Dev server starts
   ```bash
   cd chatkit-ui && npm install && npm run dev &
   sleep 5
   curl -s http://localhost:5173 | grep -q "chatkit" && echo "PASS: Dev server running" || echo "FAIL"
   ```
2. E2E: ChatKit widget renders in browser
   ```
   - Navigate to http://localhost:5173
   - Page loads without console errors
   - ChatKit chat widget is visible (empty state / greeting)
   ```
3. E2E: Session creation works
   ```
   - Open browser dev tools Network tab
   - ChatKit calls /chatkit/session (or equivalent) on load
   - Receives a valid client_secret response
   ```
4. `chatkit-ui/` is independent from `ui/` (no shared dependencies or build steps)

---

## T9: ChatKit Frontend — Bot Selector, RTL, and UX

**Description**

Add the user-facing UX to the ChatKit frontend: a bot selector dropdown (takanon/budgetkey/unified) that fetches available bots from `GET /bots`, RTL layout for Hebrew text, and proper rendering of markdown tables, links, and `download_url` references in responses. This task turns the scaffold from T8 into a production-ready chat interface.

**Files to edit**

1. `chatkit-ui/src/App.tsx` (modify — add bot selector, RTL wrapper)
2. `chatkit-ui/src/BotSelector.tsx` (new — dropdown component fetching from /bots)
3. `chatkit-ui/src/styles.css` (new — RTL layout, Hebrew font, theming)
4. `chatkit-ui/src/chatkit-config.ts` (new — ChatKit theme config, session management per bot)

**Needed knowledge**

- ChatKit custom theming: https://developers.openai.com/api/docs/guides/chatkit-themes
- ChatKit widgets for rich content: https://developers.openai.com/api/docs/guides/chatkit-widgets
- CSS `direction: rtl` for Hebrew layout, `font-family` for Hebrew-appropriate fonts
- The `GET /bots` endpoint from T7 returns `[{"slug": "takanon", "name": "...", "description": "..."}, ...]`
- Bot responses include markdown tables and `download_url` links (see `specs/budgetkey/agent.txt` for output format)

**Definition of done**

1. E2E: Bot selector works
   ```
   - Navigate to http://localhost:5173
   - Dropdown/selector shows 3 bots: takanon, budgetkey, unified
   - Selecting a bot changes the chat context (new session)
   ```
2. E2E: Full Hebrew conversation
   ```
   - Select "takanon" bot
   - Type: "מהו סעיף 106 לתקנון הכנסת?"
   - Response streams in RTL (right-to-left text alignment)
   - Hebrew characters render correctly (no mojibake)
   - Response contains legal content with citations
   ```
3. E2E: Markdown and links render
   ```
   - Select "budgetkey" bot
   - Type: "כמה תקציב קיבל משרד החינוך?"
   - Markdown tables render as formatted tables (not raw pipes)
   - download_url links are clickable <a> tags
   ```
4. E2E: RTL layout
   ```
   - Chat bubbles align to the right for user messages
   - Input field has dir="rtl"
   - Bot name and header text flow right-to-left
   ```

---

## T10: Observability and Logging

**Description**

The current `assistant_loop.py` writes detailed per-conversation logs to `benchmark/log.txt` including: every tool call name and arguments, tool outputs, file search results, and the final assistant response. The new `response_loop.py` must provide equivalent observability. Add structured logging (JSON lines) to `response_loop.py` that captures all tool calls, their inputs/outputs, round count, token usage, and timing. Also add a request-level logging middleware to the FastAPI chat endpoint so each conversation is traceable.

**Files to edit**

1. `botnim/response_loop.py` (add structured logging)
2. `backend/api/server.py` (add request logging middleware for /chat endpoints)

**Needed knowledge**

- The current logging pattern in `assistant_loop.py` lines 48-51 (log file init), 62-63 (question), 92-93 (tool calls), 131-134 (tool inputs), 185-193 (tool outputs), 117-121 (final response)
- Python `logging` module with JSON formatter, or structured logging library like `structlog`
- FastAPI middleware pattern for request/response logging
- The `ResponseLoopResult.tool_calls` list from T3 already captures tool names and arguments — this task adds persistent file/stream logging

**Definition of done**

1. E2E: Log file produced after CLI conversation
   ```bash
   echo "מהו סעיף 106?" | botnim assistant --bot takanon --environment staging
   # Check that a log file was created
   python -c "
   import json, glob
   logs = glob.glob('logs/*.jsonl') or glob.glob('benchmark/log.jsonl')
   assert len(logs) > 0, 'No log files found'
   with open(logs[-1]) as f:
       entries = [json.loads(line) for line in f if line.strip()]
   tool_entries = [e for e in entries if e.get('event') == 'tool_call']
   assert len(tool_entries) > 0, 'No tool call log entries'
   print(f'PASS: Found {len(entries)} log entries, {len(tool_entries)} tool calls')
   "
   ```
2. E2E: Chat API request produces traceable log
   ```bash
   curl -X POST http://localhost:8000/chat \
     -H "Content-Type: application/json" \
     -d '{"bot_slug": "takanon", "message": "מהו סעיף 106?"}'
   # Server logs should contain: request_id, bot_slug, tool calls, response time
   # Check server stdout/stderr or log file
   ```
3. Each log entry contains: timestamp, request_id (UUID), bot_slug, event_type, and relevant payload
4. Tool call log entries contain: tool_name, arguments, output (truncated to 500 chars), duration_ms

---

## T11: Automated Test Suite

**Description**

Create pytest tests for the three new core modules: `bot_config`, `tool_executor`, and `response_loop`. The project already has `pytest.ini` configured but zero test files exist. Tests should be runnable in CI with `pytest` and cover: config loading for all bots, tool dispatch routing, tool error handling, and a live integration test for the response loop. Use `pytest.mark` to separate fast unit tests from slow integration tests that hit live APIs.

**Files to edit**

1. `tests/conftest.py` (new — shared fixtures: OpenAI client, bot configs)
2. `tests/test_bot_config.py` (new — config loading tests)
3. `tests/test_tool_executor.py` (new — tool dispatch and error handling tests)
4. `tests/test_response_loop.py` (new — integration tests against live APIs)

**Needed knowledge**

- Existing `pytest.ini` at project root: `asyncio_mode = auto`, `log_cli = true`
- `AVAILABLE_BOTS` from `config.py` gives the list of bot slugs to test against
- Tests that hit OpenAI or ES need live credentials — mark with `@pytest.mark.integration`
- `conftest.py` should provide `@pytest.fixture` for `openai_client` and `bot_config` parameterized by bot slug

**Definition of done**

1. E2E: Unit tests pass
   ```bash
   pytest tests/test_bot_config.py tests/test_tool_executor.py -v --ignore-glob="*integration*"
   # All tests pass, no live API calls needed
   ```
2. E2E: Integration tests pass
   ```bash
   pytest tests/test_response_loop.py -v -m integration
   # At least 2 tests: one takanon question, one budgetkey question
   # Both return non-empty answers with expected tool calls
   ```
3. E2E: Full suite
   ```bash
   pytest tests/ -v
   # All tests pass, exit code 0
   # Coverage report shows bot_config.py, tool_executor.py, response_loop.py covered
   ```
4. Tests are parameterized across bots where applicable (e.g., `@pytest.mark.parametrize("bot_slug", AVAILABLE_BOTS)` for config loading)

---

## T12: NGINX Config and Docker Compose Rewrite

**Description**

Rewrite the Docker Compose stack and NGINX configuration to remove all LibreChat dependencies (LibreChat API, MongoDB, MeiliSearch, init-user) and add the new ChatKit frontend container. NGINX must route `/` to the ChatKit frontend, `/api/` to the botnim API, and `/botnim/` to the botnim API (backward compat). Update both `docker-compose.local.yml` and `docker-compose.minimal.yml`. The NGINX config currently lives at `LibreChat/client/nginx.conf` — it must be moved to a standalone location since LibreChat is being removed. Also delete the `scripts/` directory containing LibreChat-specific MongoDB initialization scripts (`init-user.js`, `init-user.sh`, `mongo-init.js`) that are referenced by the current docker-compose `init-user` service.

**Files to edit**

1. `docker-compose.local.yml` (remove LibreChat services, add chatkit-ui, update nginx volume)
2. `docker-compose.minimal.yml` (same changes)
3. `nginx/default.conf` (new — standalone NGINX config, replaces `LibreChat/client/nginx.conf`)
4. `scripts/` directory (delete `init-user.js`, `init-user.sh`, `mongo-init.js` — LibreChat MongoDB init scripts)

**Needed knowledge**

- Current `docker-compose.local.yml` services: elasticsearch, kibana, mongodb, init-user, meilisearch, api (LibreChat), botnim_api, nginx — 8 services
- After migration: elasticsearch, kibana (optional), botnim_api, chatkit-ui, nginx — 4-5 services
- Current NGINX routes: `/` and `/api/` → LibreChat (port 3080), `/botnim/` → botnim_api (port 8000)
- New NGINX routes: `/` → chatkit-ui (port 5173 or static files), `/api/` → botnim_api (port 8000), `/botnim/` → botnim_api (backward compat)
- Current NGINX config is at `LibreChat/client/nginx.conf` — this path disappears when LibreChat is removed

**Definition of done**

1. E2E: Stack starts with no LibreChat containers
   ```bash
   docker compose -f docker-compose.local.yml up -d
   sleep 30  # wait for ES healthcheck
   docker ps --format '{{.Names}}' | sort
   # Should list: es01, kibana (optional), botnim_api, chatkit-ui, nginx
   # Should NOT list: LibreChat-API, chat-mongodb, chat-meilisearch, init-user
   ```
2. E2E: NGINX routes correctly
   ```bash
   # Frontend
   curl -s http://localhost/ | grep -q "chatkit" && echo "PASS: / routes to ChatKit" || echo "FAIL"
   # API
   curl -s http://localhost/api/bots | python -c "import sys,json; print('PASS:', json.load(sys.stdin))" || echo "FAIL"
   # Backward compat
   curl -s "http://localhost/botnim/retrieve/takanon/legal_text?query=test" && echo "PASS: /botnim/ route works" || echo "FAIL"
   ```
3. Removed volumes: `mongodata`, `meilidata`, `librechat_images`, `librechat_logs`
4. No references to `LibreChat/` directory remain in any docker-compose file

---

## T13: Cleanup and Decommission Legacy Code

**Description**

Remove all legacy Assistants API code. Delete `vector_store_openai.py`, delete `benchmark/assistant_loop.py`, remove stale imports, and archive or delete the `LibreChat/` directory (which contains deployment scripts `push.sh`, `reload.sh`, `sync-production.sh`, `rerun.sh`, `sync-staging.sh` — these are replaced by T14). This is the final task — only execute after all other tasks pass their DoD. Verify the full system works end-to-end with zero `client.beta.assistants/threads/runs` calls remaining anywhere in the codebase.

**Important note**: `botnim/document_parser/wikitext/extract_structure.py` line 82 uses `client.beta.chat.completions.parse()` — this is the **Structured Outputs beta**, NOT the Assistants API. Do NOT delete or modify this. The grep check below explicitly excludes it.

**Files to edit**

1. `botnim/vector_store/vector_store_openai.py` (delete)
2. `botnim/benchmark/assistant_loop.py` (delete)
3. `botnim/vector_store/__init__.py` (remove `VectorStoreOpenAI` import if present)
4. `requirements.txt` (remove `requests-openapi` if no longer used, or keep if tool_executor still uses it)

**Needed knowledge**

- Which files import from `openai.types.beta.threads` — search for `beta.threads`, `beta.assistants`, `ToolCallsStepDetails`
- `VectorStoreOpenAI` in `vector_store_openai.py` — verify it has no remaining callers
- `assistant_loop.py` — verify `response_loop.py` has fully replaced it (runner.py, cli_assistant.py no longer import it)
- The `LibreChat/` directory at the repo root — confirm all its functionality is replaced by ChatKit + botnim API. Contains 11 shell scripts including deployment tooling — ensure T14 replacements are in place before deleting.
- `extract_structure.py` uses `client.beta.chat.completions.parse()` — this is Structured Outputs (a different beta namespace), not Assistants API. Leave it alone.

**Definition of done**

1. E2E: No Assistants API references remain (excluding Structured Outputs beta)
   ```bash
   # Search for Assistants API patterns, explicitly excluding extract_structure.py
   grep -rn "beta\.assistants\|beta\.threads\|beta\.runs\|ToolCallsStepDetails\|assistant_loop\|VectorStoreOpenAI" \
     botnim/ backend/ --include="*.py" \
     --exclude="extract_structure.py" \
     && echo "FAIL: Legacy references found" || echo "PASS: Clean codebase"
   ```
2. E2E: `extract_structure.py` still works (Structured Outputs beta preserved)
   ```bash
   python -c "
   from botnim.document_parser.wikitext.extract_structure import extract_structure
   # Import succeeds — the beta.chat.completions.parse call was not broken
   print('PASS: extract_structure module intact')
   "
   ```
3. E2E: Full benchmark suite passes
   ```bash
   botnim benchmarks staging takanon --local --select all --concurrency 3
   botnim benchmarks staging budgetkey --local --select all --concurrency 3
   # Both complete without error
   ```
4. E2E: Full test suite passes
   ```bash
   pytest tests/ -v
   # All tests pass, exit code 0
   ```
5. E2E: Full docker stack works end-to-end
   ```bash
   docker compose -f docker-compose.local.yml up -d
   sleep 30
   curl -s http://localhost/ | grep -q "chatkit"
   curl -X POST http://localhost/api/chat \
     -H "Content-Type: application/json" \
     -d '{"bot_slug": "takanon", "message": "מהו סעיף 106?"}' \
     | python -c "import sys,json; r=json.load(sys.stdin); assert len(r['response_text'])>100; print('PASS')"
   ```
6. Deleted files no longer exist:
   ```bash
   test ! -f botnim/vector_store/vector_store_openai.py && echo "PASS" || echo "FAIL"
   test ! -f botnim/benchmark/assistant_loop.py && echo "PASS" || echo "FAIL"
   ```

---

## T14: Production Deployment Tooling

**Description**

The only production deployment tooling lives inside `LibreChat/` — scripts that are deleted in T13. Before decommissioning LibreChat, create replacement deployment scripts for the new stack. The current deployment model is: SSH into a VM, pull Docker images, copy `.env` + config files, run `docker compose up`. The new scripts must do the same but for the post-migration stack (no LibreChat, no MongoDB, no MeiliSearch — just ES, botnim_api, chatkit-ui, NGINX).

Current scripts being replaced:
- `LibreChat/push.sh` — SCPs config files to production server, SSHes in, runs `reload.sh`
- `LibreChat/reload.sh` — Pulls botnim-api image, downloads LibreChat from GitHub, copies `.env`/configs, runs `rerun.sh`
- `LibreChat/rerun.sh` — Runs `docker compose -f deploy-compose.yml up`
- `LibreChat/sync-production.sh` / `sync-staging.sh` — Copies `.env` and OpenAPI spec to production/staging, triggers reload

The new scripts are simpler because there's no LibreChat to download/build — just pull images and `docker compose up`.

**Files to edit**

1. `deploy/deploy.sh` (new — replaces `push.sh` + `reload.sh`: pull images, copy configs, restart stack)
2. `deploy/deploy-compose.yml` (new — production Docker Compose, replaces `LibreChat/deploy-compose.yml`)
3. `deploy/sync-data.sh` (new — replaces `sync-production.sh`/`sync-staging.sh`: copies OpenAPI spec + triggers ES re-sync)
4. `deploy/README.md` (new — documents deployment process, server prerequisites, required `.env` vars)

**Needed knowledge**

- Current deployment target: SSH to `bonim` (production) or `botnim-staging` (staging) — host aliases defined in `~/.ssh/config`
- Production server has Docker + Docker Compose pre-installed
- `.env.production` lives on the server (not in git) — contains ES credentials, OpenAI API key, Firebase config
- `serviceAccountKey.json` lives on the server — Firebase auth credentials
- The `deploy-compose.yml` (production) differs from `docker-compose.local.yml` (local dev) — production may include different ES config, resource limits, restart policies
- No GitHub Actions CI/CD currently runs tests — deployment is manual via these scripts

**Definition of done**

1. E2E: Deploy to staging succeeds
   ```bash
   ./deploy/deploy.sh staging
   # Script should:
   # 1. SCP deploy-compose.yml and NGINX config to staging server
   # 2. SSH in, pull latest botnim-api image
   # 3. Run docker compose up -d
   # 4. Health check: curl the staging URL for 200 response
   ```
2. E2E: Data sync to staging succeeds
   ```bash
   ./deploy/sync-data.sh staging
   # Script should:
   # 1. Copy OpenAPI spec to staging server
   # 2. SSH in and trigger ES re-index via botnim sync command
   ```
3. E2E: Deploy script rejects unknown environments
   ```bash
   ./deploy/deploy.sh unknown 2>&1 | grep -q "Usage\|staging\|production" \
     && echo "PASS: Usage message shown" || echo "FAIL"
   ```
4. `deploy/README.md` documents: server prerequisites, required files on server (`.env.production`, `serviceAccountKey.json`), how to deploy, how to rollback (manual `docker compose down && up` with previous image tag)

---

## Risks & Notes

Items that need human decision or are outside the scope of the migration tasks:

### 1. `extract_structure.py` uses `client.beta.chat.completions.parse()`

**File**: `botnim/document_parser/wikitext/extract_structure.py:82`

This is the **Structured Outputs beta** endpoint, NOT the Assistants API. It is unrelated to this migration. However, OpenAI may graduate this to a stable endpoint (e.g., `client.chat.completions.parse()`). No action needed now, but track for a future `openai` library upgrade.

### 2. No CI/CD Test Gate

Per `TEST-STATUS.md`, no GitHub Actions workflow runs `pytest`. A push to `main` triggers deployment with zero automated quality checks. The migration adds significant new code (response loop, backend chat API, ChatKit frontend) without any CI safety net. Consider adding a GitHub Actions workflow that runs `pytest tests/` before deploy.

### 3. Production Server Configuration

The `DELTAS.md` documents many unknowns about the production environment (server IPs, SSL termination, `.env.production` contents, `serviceAccountKey.json` location). T14 creates deployment scripts, but they require answers to the questions in `DELTAS.md` sections 1-3 to actually work.

### 4. ChatKit Availability and API Stability

ChatKit (`@openai/chatkit-react`) is relatively new. API surface may change. T8 and T9 should pin exact package versions in `package.json` and document the ChatKit version used.

### 5. LibreChat User Data

If LibreChat's MongoDB contains user conversation history that needs preserving, archive the MongoDB data before deleting LibreChat containers (T12). The system prompts forbid cross-turn memory, so conversation history likely has no functional value — but confirm with stakeholders before permanent deletion.
2. E2E: Full benchmark suite passes
   ```bash
   botnim benchmarks staging takanon --local --select all --concurrency 3
   botnim benchmarks staging budgetkey --local --select all --concurrency 3
   # Both complete without import errors, API errors, or score regression
   ```
3. E2E: Full test suite passes
   ```bash
   pytest tests/ -v
   # All tests pass, exit code 0
   ```
4. E2E: Full docker stack works end-to-end
   ```bash
   docker compose -f docker-compose.local.yml up -d
   sleep 30
   # Frontend loads
   curl -s http://localhost/ | grep -q "chatkit"
   # Chat works
   curl -X POST http://localhost/api/chat \
     -H "Content-Type: application/json" \
     -d '{"bot_slug": "takanon", "message": "מהו סעיף 106?"}' \
     | python -c "import sys,json; r=json.load(sys.stdin); assert len(r['response_text'])>100; print('PASS')"
   ```
5. Deleted files no longer exist:
   ```bash
   test ! -f botnim/vector_store/vector_store_openai.py && echo "PASS" || echo "FAIL"
   test ! -f botnim/benchmark/assistant_loop.py && echo "PASS" || echo "FAIL"
   ```
