"""GPT-4o judge for sanity_runs.

Two single-row functions plus a `judge_all` orchestrator. Both single-row
functions have early-exit short-circuits for failed captures so we don't
burn API spend on empty answers, and a JSON-parse-fallback to INFRA so a
single bad model output can't crash the whole run.

The model is hardcoded to gpt-4o because its Hebrew + JSON-schema support
are the load-bearing reasons for choosing OpenAI here. If we ever want
to bump it, do that with config, not by adding a parameter.
"""
from __future__ import annotations

import json
import logging
from typing import Optional

from openai import OpenAI

from botnim.sanity.types import (
    ABVerdict,
    CaptureRow,
    JudgedRow,
    RubricVerdict,
)

logger = logging.getLogger(__name__)

_MODEL = "gpt-4o"

_AB_SCHEMA = {
    "type": "json_schema",
    "json_schema": {
        "name": "ab_verdict",
        "schema": {
            "type": "object",
            "properties": {
                "verdict": {"type": "string", "enum": ["NEW", "OLD", "TIE"]},
                "reason": {"type": "string"},
            },
            "required": ["verdict", "reason"],
            "additionalProperties": False,
        },
        "strict": True,
    },
}

_RUBRIC_SCHEMA = {
    "type": "json_schema",
    "json_schema": {
        "name": "rubric_verdict",
        "schema": {
            "type": "object",
            "properties": {
                "score": {"type": "number", "minimum": 0, "maximum": 1},
                "verdict": {"type": "string", "enum": ["PASS", "FAIL", "XFAIL"]},
                "reason": {"type": "string"},
            },
            "required": ["score", "verdict", "reason"],
            "additionalProperties": False,
        },
        "strict": True,
    },
}


def _client() -> OpenAI:
    return OpenAI()  # reads OPENAI_API_KEY from env, same as the rest of botnim


def judge_ab(row: CaptureRow) -> ABVerdict:
    if not row.answer_new.ok or not row.answer_new.text.strip():
        err = row.answer_new.error or "no answer"
        return ABVerdict(verdict="OLD", reason=f"NEW failed to respond — {err}")
    if not row.answer_old.ok or not row.answer_old.text.strip():
        return ABVerdict(verdict="NEW", reason="OLD failed to respond — NEW returned a real answer.")

    user = (
        f"Question (Hebrew):\n{row.question}\n\n"
        f"Expected behavior (English rubric):\n{row.expected_behavior}\n\n"
        f"OLD bot answer:\n{row.answer_old.text}\n\n"
        f"NEW bot answer:\n{row.answer_new.text}\n\n"
        "Decide which answer better addresses the user's actual question. "
        "NEW wins if it is more correct, more complete, more honest about gaps, "
        "or otherwise better-aligned with what a real user needs. OLD wins if "
        "the legacy answer is. TIE if both are roughly equivalent — use sparingly. "
        "Reasons must be specific and short, naming what the loser got wrong."
    )
    return _call_and_parse(user_msg=user, schema=_AB_SCHEMA, parser=_parse_ab)


def judge_rubric(row: CaptureRow) -> RubricVerdict:
    if not row.answer_new.ok or not row.answer_new.text.strip():
        err = row.answer_new.error or "no answer"
        return RubricVerdict(score=None, verdict="INFRA", reason=err)

    user = (
        f"Question (Hebrew):\n{row.question}\n\n"
        f"Expected behavior (English rubric):\n{row.expected_behavior}\n\n"
        f"Must NOT contain (substrings that signal a known wrong-answer mode):\n"
        f"{row.must_not_contain}\n\n"
        f"Observed notes (Hebrew commentary from goldset author):\n{row.observed_notes}\n\n"
        f"Answer to score:\n{row.answer_new.text}\n\n"
        "Score in [0, 1]. PASS at >= 0.8 (covers rubric, may miss non-load-bearing token). "
        "FAIL below 0.8. Hebrew variants (niqqud, ועדת/וועדת, synonyms) do NOT lose points. "
        "If the answer triggers a must_not_contain substring, drop >= 0.4 and call it out. "
        "Use XFAIL for documented known gaps in the corpus (e.g. row 7's ועדת חקירה ממלכתית "
        "vs פרלמנטרית confusion when admitting no access to חוק ועדות חקירה; row 10's "
        "inability to retrieve חוק חובת המכרזים)."
    )
    return _call_and_parse(user_msg=user, schema=_RUBRIC_SCHEMA, parser=_parse_rubric)


def judge_all(rows: list[CaptureRow]) -> dict[str, dict]:
    """Returns a row-keyed dict that matches the JudgedRow shape but as
    a plain dict (so it serialises into the judged_json column verbatim)."""
    out: dict[str, dict] = {}
    for row in rows:
        ab = judge_ab(row)
        rubric = judge_rubric(row)
        out[str(row.row)] = {
            "ab_verdict": ab.verdict,
            "ab_reason": ab.reason,
            "rubric_score": rubric.score,
            "rubric_verdict": rubric.verdict,
            "rubric_reason": rubric.reason,
        }
    return out


def _call_and_parse(*, user_msg: str, schema: dict, parser):
    try:
        resp = _client().chat.completions.create(
            model=_MODEL,
            response_format=schema,
            messages=[
                {"role": "system", "content": "You are an expert Hebrew-language legal-text evaluator."},
                {"role": "user", "content": user_msg},
            ],
        )
        content = resp.choices[0].message.content
        return parser(content)
    except Exception as e:  # noqa: BLE001 — we want all failures to fall through to INFRA
        logger.warning("judge call failed: %s: %s", type(e).__name__, e)
        return parser(None, error=str(e))


def _parse_ab(content: Optional[str], *, error: Optional[str] = None) -> ABVerdict:
    if content is None or error is not None:
        return ABVerdict(verdict="TIE", reason=f"judge call failed: {error}")
    try:
        data = json.loads(content)
        return ABVerdict(verdict=data["verdict"], reason=data["reason"])
    except (json.JSONDecodeError, KeyError) as e:
        return ABVerdict(verdict="TIE", reason=f"judge response unparseable: {type(e).__name__}")


def _parse_rubric(content: Optional[str], *, error: Optional[str] = None) -> RubricVerdict:
    if content is None or error is not None:
        return RubricVerdict(score=None, verdict="INFRA", reason=f"judge call failed: {error}")
    try:
        data = json.loads(content)
        return RubricVerdict(
            score=float(data["score"]),
            verdict=data["verdict"],
            reason=data["reason"],
        )
    except (json.JSONDecodeError, KeyError, ValueError) as e:
        return RubricVerdict(
            score=None,
            verdict="INFRA",
            reason=f"judge response unparseable: {type(e).__name__}",
        )
