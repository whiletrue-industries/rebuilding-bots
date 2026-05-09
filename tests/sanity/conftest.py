"""Shared fixtures for the botnim.sanity test suite."""
from __future__ import annotations

import pytest

from botnim.sanity.types import RunSummary


def _summary(**overrides) -> RunSummary:
    base = dict(
        total_rows=11,
        ab_new_wins=5,
        ab_old_wins=3,
        ab_ties=3,
        rubric_pass=8,
        rubric_fail=1,
        rubric_xfail=2,
        rubric_infra=0,
        pass_rate=8 / (8 + 1),
    )
    base.update(overrides)
    return RunSummary(**base)


@pytest.fixture
def make_summary():
    """Factory: returns a RunSummary with defaults overridable per call."""
    return _summary
