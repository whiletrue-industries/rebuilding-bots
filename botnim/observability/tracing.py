"""Phoenix OTel bootstrap. No-op unless PHOENIX_COLLECTOR_ENDPOINT is set."""
from __future__ import annotations
import os
import re
import logging
from typing import TYPE_CHECKING, Dict
from opentelemetry.sdk.trace import SpanProcessor, ReadableSpan, Span

if TYPE_CHECKING:
    from fastapi import FastAPI

logger = logging.getLogger(__name__)

_SECRET_KEYS = frozenset({
    "http.request.header.authorization",
    "http.request.header.cookie",
    "http.request.header.set-cookie",
    "http.request.header.x-api-key",
    "http.request.header.openai-api-key",
    "http.response.header.set-cookie",
    "openai.api_key",
    "db.connection_string",
})
_VALUE_PATTERNS = [
    re.compile(r"sk-[A-Za-z0-9_\-]{12,}"),
    re.compile(r"Bearer\s+[A-Za-z0-9_\-\.]+", re.I),
    re.compile(r"session=[^;\s]+", re.I),
    re.compile(r"://[^/@:]+:[^@]+@"),
    re.compile(r"api[_-]?key=[^&\s]+", re.I),
]
_REDACTED = "[REDACTED]"


class SecretScrubbingSpanProcessor(SpanProcessor):
    def on_start(self, span: Span, parent_context=None) -> None: pass
    def on_end(self, span: ReadableSpan) -> None:
        attrs: Dict = getattr(span, "_attributes", None) or {}
        for k in list(attrs.keys()):
            if k.lower() in _SECRET_KEYS:
                attrs[k] = _REDACTED
                continue
            v = attrs[k]
            if isinstance(v, str):
                redacted = v
                for p in _VALUE_PATTERNS:
                    redacted = p.sub(_REDACTED, redacted)
                if redacted != v:
                    attrs[k] = redacted
    def shutdown(self) -> None: pass
    def force_flush(self, timeout_millis: int = 30000) -> bool: return True


def init_tracing(app: "FastAPI") -> None:
    endpoint = os.getenv("PHOENIX_COLLECTOR_ENDPOINT")
    if not endpoint:
        logger.debug("PHOENIX_COLLECTOR_ENDPOINT unset; tracing disabled")
        return
    from phoenix.otel import register
    from openinference.instrumentation.openai import OpenAIInstrumentor
    from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor
    from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

    env = os.getenv("ENVIRONMENT", "unknown")
    # Write to Phoenix's "default" project (no explicit project_name) so the
    # bot's spans land in the SAME project as LibreChat's chat.turn root.
    # The LibreChat admin trace fetch at /api/botnim/traces/<id> uses
    # single-project lookup (it breaks out at the first project that has
    # spans for the given trace_id); aligning projects is the simplest way
    # to surface bot-internal spans (rrf.fuse, embed, SELECT documents)
    # in the merged trace timeline. LibreChat uses serviceName-only export
    # and lands in "default" too in Phoenix v15 (which doesn't auto-route
    # by service.name without openinference.project.name).
    #
    # IMPORTANT — DO NOT call register(batch=True) then add_span_processor.
    # Phoenix's register() prints a warning we missed for months:
    #   "Using a default SpanProcessor. add_span_processor will overwrite
    #   this default."
    # That replaces (not appends!) the OTLP-exporting BatchSpanProcessor
    # with our scrubber, so every bot span gets scrubbed-then-dropped.
    # Mirror LibreChat's pattern: build the BatchSpanProcessor + scrubber
    # ourselves and add BOTH explicitly.
    from opentelemetry.sdk.trace.export import BatchSpanProcessor
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
    tracer_provider = register(
        endpoint=endpoint,
        set_global_tracer_provider=True,
        batch=False,  # we wire the exporter manually below
        protocol="http/protobuf",
    )
    tracer_provider.add_span_processor(
        BatchSpanProcessor(OTLPSpanExporter(endpoint=endpoint))
    )
    tracer_provider.add_span_processor(SecretScrubbingSpanProcessor())

    try:
        OpenAIInstrumentor().instrument(tracer_provider=tracer_provider)
    except Exception as e:
        # openinference-instrumentation-openai 0.1.18 broke with wrapt 2.x
        # ("wrap_function_wrapper() got an unexpected keyword argument
        # 'module'"). Don't let that take down the rest of the pipeline —
        # FastAPI / SQLAlchemy spans are the load-bearing ones for the
        # admin trace UI. Bump openinference once a compatible release
        # lands.
        logger.warning("OpenAI instrumentation skipped: %s", e)
    FastAPIInstrumentor.instrument_app(
        app, tracer_provider=tracer_provider,
        excluded_urls="health,healthz",
        http_capture_headers_server_request=[],
        http_capture_headers_server_response=[],
    )
    try:
        from botnim.db.session import get_engine
        SQLAlchemyInstrumentor().instrument(
            engine=get_engine(),
            tracer_provider=tracer_provider,
            enable_commenter=False,
        )
    except Exception as e:
        # Engine not available at init time on some boot paths — skip SQLA
        # rather than crash the API; spans for SQLA will be missing but
        # FastAPI + OpenAI are still covered.
        logger.warning("SQLAlchemy instrumentation skipped: %s", e)
    logger.info("phoenix tracing initialized: endpoint=%s project=default env=%s", endpoint, env)
