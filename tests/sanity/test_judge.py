"""Unit tests for botnim.sanity.judge.

OpenAI client is mocked at the openai.OpenAI boundary. We assert on the
prompts, the response_format schema, and the output parsing — not on
real API calls.
"""
from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from botnim.sanity import judge
from botnim.sanity.types import Answer, CaptureRow

FIXTURES = Path(__file__).parent / "fixtures"


def _make_row(row: int = 0, **kwargs) -> CaptureRow:
    base = dict(
        row=row,
        question="מתי תועדת הכנסת מצביעה על תקציב?",
        expected_behavior="Cites §78 and explains the budget vote sequence.",
        must_not_contain=["איזה סוג מידע אתם מחפשים"],
        observed_notes="",
        answer_old=Answer(text="ועדת הכנסת מצביעה על התקציב.", ok=True),
        answer_new=Answer(text="לפי §78(ג) של תקנון הכנסת…", ok=True),
    )
    base.update(kwargs)
    return CaptureRow(**base)


def test_judge_ab_returns_parsed_verdict(monkeypatch):
    fake = MagicMock()
    fake.chat.completions.create.return_value.choices = [
        MagicMock(message=MagicMock(content=(FIXTURES / "openai_ab_response.json").read_text()))
    ]
    monkeypatch.setattr(judge, "_client", lambda: fake)

    row = _make_row()
    out = judge.judge_ab(row)

    assert out.verdict == "NEW"
    assert "§78" in out.reason
    # Confirm the call carried the intended schema and the question text
    args, kwargs = fake.chat.completions.create.call_args
    assert kwargs["model"] == "gpt-4o"
    assert kwargs["response_format"]["type"] == "json_schema"
    assert kwargs["response_format"]["json_schema"]["name"] == "ab_verdict"
    user_messages = [m for m in kwargs["messages"] if m["role"] == "user"]
    assert any("§78" in m["content"] or "תועדת הכנסת" in m["content"] for m in user_messages)


def test_judge_ab_old_wins_default_when_new_failed(monkeypatch):
    """If answer_new.ok is False with empty text, judge_ab returns OLD without
    an API call."""
    fake = MagicMock()
    monkeypatch.setattr(judge, "_client", lambda: fake)

    row = _make_row(
        answer_new=Answer(text="", ok=False, error="timeout"),
        answer_old=Answer(text="real OLD answer", ok=True),
    )
    out = judge.judge_ab(row)

    assert out.verdict == "OLD"
    assert "NEW failed" in out.reason or "no answer" in out.reason.lower()
    fake.chat.completions.create.assert_not_called()


def test_judge_rubric_returns_parsed(monkeypatch):
    fake = MagicMock()
    fake.chat.completions.create.return_value.choices = [
        MagicMock(message=MagicMock(content=(FIXTURES / "openai_rubric_response.json").read_text()))
    ]
    monkeypatch.setattr(judge, "_client", lambda: fake)

    row = _make_row()
    out = judge.judge_rubric(row)

    assert out.verdict == "PASS"
    assert out.score == pytest.approx(0.85)
    assert "law" in out.reason.lower() or "section" in out.reason.lower()


def test_judge_rubric_infra_when_new_empty(monkeypatch):
    fake = MagicMock()
    monkeypatch.setattr(judge, "_client", lambda: fake)

    row = _make_row(answer_new=Answer(text="", ok=False, error="timeout"))
    out = judge.judge_rubric(row)

    assert out.verdict == "INFRA"
    assert out.score is None
    assert "timeout" in out.reason
    fake.chat.completions.create.assert_not_called()


def test_judge_rubric_xfail_row_7_known_failure(monkeypatch):
    """Row 7's known XFAIL pattern: NEW confuses ממלכתית with פרלמנטרית AND
    admits no access to חוק ועדות חקירה."""
    response = json.dumps({
        "score": 0.30, "verdict": "XFAIL",
        "reason": "Confuses ממלכתית with פרלמנטרית and admits no access to חוק ועדות חקירה — documented row-7 gap.",
    })
    fake = MagicMock()
    fake.chat.completions.create.return_value.choices = [
        MagicMock(message=MagicMock(content=response))
    ]
    monkeypatch.setattr(judge, "_client", lambda: fake)
    row = _make_row(row=7)
    out = judge.judge_rubric(row)
    assert out.verdict == "XFAIL"
    assert out.score == pytest.approx(0.30)


def test_judge_all_returns_per_row_dict(monkeypatch):
    """judge_all wires the two single-row functions into a dict keyed by
    row index — used by the runner."""
    ab_fixture = (FIXTURES / "openai_ab_response.json").read_text()
    rubric_fixture = (FIXTURES / "openai_rubric_response.json").read_text()
    fake = MagicMock()
    fake.chat.completions.create.side_effect = [
        MagicMock(choices=[MagicMock(message=MagicMock(content=ab_fixture))]),
        MagicMock(choices=[MagicMock(message=MagicMock(content=rubric_fixture))]),
    ]
    monkeypatch.setattr(judge, "_client", lambda: fake)

    rows = [_make_row(row=0)]
    judged = judge.judge_all(rows)

    assert "0" in judged
    assert judged["0"]["ab_verdict"] == "NEW"
    assert judged["0"]["rubric_verdict"] == "PASS"


def test_judge_handles_unparseable_response_as_infra(monkeypatch):
    """If GPT-4o emits non-JSON or schema-invalid output, fall back to INFRA
    rather than crashing the whole run."""
    fake = MagicMock()
    fake.chat.completions.create.return_value.choices = [
        MagicMock(message=MagicMock(content="not json at all"))
    ]
    monkeypatch.setattr(judge, "_client", lambda: fake)

    row = _make_row()
    out = judge.judge_rubric(row)
    assert out.verdict == "INFRA"
    assert "parse" in out.reason.lower() or "json" in out.reason.lower()
