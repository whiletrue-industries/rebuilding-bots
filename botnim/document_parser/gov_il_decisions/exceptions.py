"""Exceptions raised by the gov_il_decisions fetcher.

EmptyUpstreamIndex is reused from the shared pdfs.exceptions module for
consistency with how the rest of the fetcher fleet signals "upstream
returned nothing — refuse to overwrite last-known-good".

CategorizationError is raised by categorize.py when the LLM repeatedly
returns labels outside the controlled vocab; the orchestrator catches it
and falls back to the generic ('אחר', 'כללי') pair.
"""
from ..pdfs.exceptions import EmptyUpstreamIndex  # noqa: F401  (re-export)


class CategorizationError(Exception):
    """LLM categorization failed even after one retry."""
    pass


class GovIlApiError(Exception):
    """gov.il returned an unexpected (non-JSON) response.

    The gov.il SPA serves an HTML shell with HTTP 200 when an API path
    moves (as happened in the 2026-05 migration off
    ``www.gov.il/CollectorsWebApi`` to the ``openapi-gc.digital.gov.il``
    gateway) or when the request is WAF-blocked. Calling ``.json()`` on
    that body raises an opaque ``JSONDecodeError`` — which is exactly what
    silently hid the migration for a month. The client raises this instead,
    naming the URL + content-type so the failure is greppable and loud.
    """
    pass
