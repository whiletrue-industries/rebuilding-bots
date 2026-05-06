"""Tests for the Word-doc tool's request/response pydantic models."""
from __future__ import annotations

import pytest
from pydantic import ValidationError

from botnim.word_doc.models import (
    WordDocSection,
    WordDocRequest,
    WordDocResponse,
)


def test_section_valid():
    s = WordDocSection(heading="רקע", level=1, body_md="טקסט")
    assert s.heading == "רקע"


def test_section_level_clamped_to_1_3():
    with pytest.raises(ValidationError):
        WordDocSection(heading="x", level=0, body_md="")
    with pytest.raises(ValidationError):
        WordDocSection(heading="x", level=4, body_md="")
    for n in (1, 2, 3):
        WordDocSection(heading="x", level=n, body_md="")


def test_request_requires_at_least_one_section():
    with pytest.raises(ValidationError):
        WordDocRequest(title="x", sections=[])


def test_request_title_max_length():
    with pytest.raises(ValidationError):
        WordDocRequest(
            title="x" * 1001,
            sections=[WordDocSection(heading="h", level=1, body_md="b")],
        )


def test_response_shape():
    from datetime import datetime, timezone
    r = WordDocResponse(
        url="https://x.s3...",
        filename="א.docx",
        expires_at=datetime.now(timezone.utc),
    )
    assert r.url.startswith("https://")
