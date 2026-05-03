"""Categorizer unit tests.

The categorizer is one OpenAI call per decision returning a JSON object
with two keys, each constrained to a controlled vocab. This module's
contract:

* Happy path returns ``{"action_type": ..., "domain": ...}`` both in vocab.
* If the LLM returns an out-of-vocab label, retry once with a stricter prompt.
* If retry also fails, fall back to ``("אחר", "כללי")`` and log a warning
  rather than aborting the whole sync.
"""
from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from botnim.document_parser.gov_il_decisions.categorize import (
    ACTION_TYPES,
    DOMAINS,
    categorize,
)


def _mock_completion(content: str) -> MagicMock:
    response = MagicMock()
    response.choices = [MagicMock(message=MagicMock(content=content))]
    return response


def test_action_types_and_domains_match_tal_vocab():
    # Spot-check a few known-good labels from Tal's Excel
    assert "מינויים" in ACTION_TYPES
    assert "חקיקה" in ACTION_TYPES
    assert "אחר" in ACTION_TYPES  # fallback bucket must exist
    assert "ביטחון וצבא" in DOMAINS
    assert "בריאות" in DOMAINS
    assert "כללי" in DOMAINS  # fallback bucket must exist
    assert len(ACTION_TYPES) == 16
    assert len(DOMAINS) == 22


def test_categorize_happy_path():
    payload = json.dumps({"action_type": "מינויים", "domain": "בריאות"}, ensure_ascii=False)
    with patch(
        "botnim.document_parser.gov_il_decisions.categorize.get_openai_client"
    ) as get_client:
        client = MagicMock()
        client.chat.completions.create.return_value = _mock_completion(payload)
        get_client.return_value = client

        out = categorize(title="מינוי מנכ\"ל משרד הבריאות", text="מחליטים למנות...")

    assert out == {"action_type": "מינויים", "domain": "בריאות"}
    assert client.chat.completions.create.call_count == 1


def test_categorize_retries_on_out_of_vocab_then_succeeds():
    bad = json.dumps({"action_type": "פוליטיקה", "domain": "בריאות"}, ensure_ascii=False)
    good = json.dumps({"action_type": "מדיניות", "domain": "בריאות"}, ensure_ascii=False)
    with patch(
        "botnim.document_parser.gov_il_decisions.categorize.get_openai_client"
    ) as get_client:
        client = MagicMock()
        client.chat.completions.create.side_effect = [
            _mock_completion(bad),
            _mock_completion(good),
        ]
        get_client.return_value = client

        out = categorize(title="t", text="x")

    assert out == {"action_type": "מדיניות", "domain": "בריאות"}
    assert client.chat.completions.create.call_count == 2


def test_categorize_falls_back_after_two_failures():
    bad = json.dumps({"action_type": "junk", "domain": "junk"}, ensure_ascii=False)
    with patch(
        "botnim.document_parser.gov_il_decisions.categorize.get_openai_client"
    ) as get_client:
        client = MagicMock()
        client.chat.completions.create.side_effect = [
            _mock_completion(bad),
            _mock_completion(bad),
        ]
        get_client.return_value = client

        out = categorize(title="t", text="x")

    assert out == {"action_type": "אחר", "domain": "כללי"}
    assert client.chat.completions.create.call_count == 2


def test_categorize_handles_malformed_json():
    with patch(
        "botnim.document_parser.gov_il_decisions.categorize.get_openai_client"
    ) as get_client:
        client = MagicMock()
        client.chat.completions.create.side_effect = [
            _mock_completion("not json at all"),
            _mock_completion("still not json"),
        ]
        get_client.return_value = client

        out = categorize(title="t", text="x")

    assert out == {"action_type": "אחר", "domain": "כללי"}
