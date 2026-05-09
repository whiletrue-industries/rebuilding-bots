"""Tiny FastAPI stub serving a LibreChat-shaped login + chat page.

The HTML mirrors the *selectors* the real LibreChat exposes — specifically the
selectors used by scripts/ui-sanity-capture.spec.js (the canonical source of
truth):

  - Login:  input[name="email"], input[name="password"], button[type="submit"]
  - Chat input: textarea[name="text"]
  - Send: Enter key on the textarea
  - Answer container: .agent-turn (last one)
  - Stream URL shape: /api/agents/chat/stream/<id>   (SSE; fires requestfinished
    when the server closes the connection)

The client-side JS wires the textarea's Enter event to a fetch call that
targets /api/agents/chat/stream/conv1 so capture.py's requestfinished
predicate fires on exactly the right URL. The SSE response streams "שלום עולם"
then closes.

When the question is 'TRIGGER_500' the stub raises a 500 so capture.py's
error path is exercised.
"""
from __future__ import annotations

import asyncio

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, StreamingResponse


_LOGIN_HTML = """\
<!doctype html>
<html>
<head><meta charset="utf-8"/></head>
<body>
<form id="loginForm" action="/auth/login" method="post">
  <input name="email" id="email" autocomplete="email"/>
  <input name="password" id="password" type="password" autocomplete="current-password"/>
  <button type="submit" id="submit">Sign in</button>
</form>
</body>
</html>
"""

# The /c/new page.
# Key invariants that must match the JS spec selectors:
#   1. textarea[name="text"]    — chat input
#   2. .agent-turn              — where the assistant answer appears
#   3. fetch to /api/agents/chat/stream/<id>  — so requestfinished predicate fires
_CHAT_HTML = """\
<!doctype html>
<html>
<head><meta charset="utf-8"/></head>
<body>
<div id="messages">
  <!-- .agent-turn divs are injected here by JS after the stream closes -->
</div>
<textarea name="text" id="chat-input" rows="3" placeholder="Type here..."></textarea>

<script>
(function () {
  const textarea = document.querySelector('textarea[name="text"]');
  const messages = document.getElementById('messages');

  textarea.addEventListener('keydown', async function (e) {
    if (e.key !== 'Enter' || e.shiftKey) return;
    e.preventDefault();

    const question = textarea.value.trim();
    if (!question) return;
    textarea.value = '';

    // Fetch the SSE endpoint — URL shape matches the real LibreChat's
    // /api/agents/chat/stream/<convId>  which is what capture.py watches.
    const resp = await fetch('/api/agents/chat/stream/conv1?q=' + encodeURIComponent(question));

    if (!resp.ok) {
      // Insert an .agent-turn with error text so capture.py has something to read.
      const div = document.createElement('div');
      div.className = 'agent-turn';
      div.textContent = 'ERROR: ' + resp.status;
      messages.appendChild(div);
      return;
    }

    // Read the SSE body to completion (this is what triggers "requestfinished"
    // on the Playwright side — the fetch body reader exhausts).
    const reader = resp.body.getReader();
    const decoder = new TextDecoder();
    let accumulated = '';
    while (true) {
      const { value, done } = await reader.read();
      if (done) break;
      const chunk = decoder.decode(value, { stream: true });
      // Strip SSE framing (data: ... \\n\\n)
      for (const line of chunk.split('\\n')) {
        const trimmed = line.trim();
        if (trimmed.startsWith('data: ') && !trimmed.includes('[DONE]')) {
          accumulated += trimmed.slice(6);
        }
      }
    }

    // Insert the final answer as an .agent-turn div.
    const div = document.createElement('div');
    div.className = 'agent-turn';
    div.textContent = accumulated;
    messages.appendChild(div);
  });
})();
</script>
</body>
</html>
"""


def make_app() -> FastAPI:
    app = FastAPI()

    @app.get("/login", response_class=HTMLResponse)
    def login_page():
        return _LOGIN_HTML

    @app.post("/auth/login")
    async def auth_login(request: Request):
        # Accept any credentials and redirect to /c/new.
        return HTMLResponse(
            '<meta http-equiv="refresh" content="0;url=/c/new"/>',
            status_code=200,
        )

    @app.get("/c/new", response_class=HTMLResponse)
    def chat_page():
        return _CHAT_HTML

    @app.get("/api/agents/chat/stream/{conv_id}")
    async def chat_stream(conv_id: str, q: str = ""):
        if "TRIGGER_500" in q:
            return HTMLResponse("Internal Server Error", status_code=500)

        async def gen():
            for chunk in ["שלום", " ", "עולם"]:
                yield f"data: {chunk}\n\n"
                await asyncio.sleep(0.05)
            yield "data: [DONE]\n\n"

        return StreamingResponse(gen(), media_type="text/event-stream")

    return app
