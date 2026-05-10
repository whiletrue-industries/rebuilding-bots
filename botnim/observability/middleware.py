"""Trace middleware — drop forged traceparents, emit X-Phoenix-Trace-Id.

Security model:
  By default (PHOENIX_PROPAGATE_TRACE unset / falsy) incoming traceparent /
  tracestate headers are stripped so botnim-api always opens a fresh root
  span — this defends against forged trace IDs from unauthenticated callers
  (see security finding B2 in the Phoenix tracing spec).

  Set PHOENIX_PROPAGATE_TRACE=true (local dev only — never in staging/prod)
  to let botnim-api join an upstream trace from LibreChat.  This makes all
  botnim-api child spans (rrf.fuse, embeddings, sqlalchemy …) appear inside
  the same Phoenix trace as the triggering chat.turn root span.
"""
from __future__ import annotations
import os
from fastapi import FastAPI, Request
from starlette.middleware.base import BaseHTTPMiddleware
from opentelemetry import trace as otel_trace


class _TraceHeaderMiddleware(BaseHTTPMiddleware):
    def __init__(self, app, propagate: bool = False):
        super().__init__(app)
        self._propagate = propagate

    async def dispatch(self, request: Request, call_next):
        if not self._propagate:
            # Drop incoming traceparent — botnim-api always opens fresh roots
            # to defend against forged trace IDs from unauthed callers.
            scope_headers = request.scope.get("headers", [])
            request.scope["headers"] = [
                (k, v) for (k, v) in scope_headers
                if k.lower() not in (b"traceparent", b"tracestate")
            ]
        response = await call_next(request)
        span = otel_trace.get_current_span()
        ctx = span.get_span_context() if span else None
        if ctx and ctx.is_valid:
            response.headers["x-phoenix-trace-id"] = format(ctx.trace_id, "032x")
        return response


def install_trace_middleware(app: FastAPI) -> None:
    propagate = os.getenv("PHOENIX_PROPAGATE_TRACE", "").lower() == "true"
    app.add_middleware(_TraceHeaderMiddleware, propagate=propagate)
