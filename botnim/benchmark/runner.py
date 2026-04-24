"""Benchmark runner -- legacy stub.

.. deprecated:: 2026-04

    The original offline benchmark harness used the Assistants API
    (``client.beta.assistants.list`` / ``beta.threads.*``) and was scoped
    to the now-removed ``takanon`` / ``budgetkey`` bots. Both APIs retire
    2026-08-26. The harness was not on the production chat path.

    Migration target: replace with a ``response_loop`` that consumes
    :func:`botnim.bot_config.load_bot_config` and
    ``client.responses.create``. Tracked as task T6 in MIGRATION_TASKS.md.
"""
from __future__ import annotations


def run_benchmarks(environment, bots, local, reuse_answers, select, concurrency):
    """Entry point preserved for ``cli.py`` wiring. Always raises."""
    raise NotImplementedError(
        'Legacy Assistants-API benchmark runner was scoped to the removed '
        'takanon/budgetkey bots. Port to the Responses-API flow before '
        'wiring benchmarks for the unified bot (MIGRATION_TASKS.md T6).'
    )
