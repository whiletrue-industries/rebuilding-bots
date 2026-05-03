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
