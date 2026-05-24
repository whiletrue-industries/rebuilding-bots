"""GPT-4o judge for sanity_runs (two-turn aware, post-2026-05-10).

The judge sees both turn 1 and (if captured) turn 2 from each side, and
produces:
  - ABVerdict: NEW / OLD / TIE — which bot did better OVERALL across turns
  - RubricVerdict: PASS_T1 / PASS_T2 / FAIL / XFAIL / INFRA — how the NEW bot
    did against the gold-set rubric, distinguishing "got it in one turn"
    from "needed a follow-up"

The model is hardcoded to gpt-4o because its Hebrew + JSON-schema support
are the load-bearing reasons for choosing OpenAI here. If we ever want
to bump it, do that with config, not by adding a parameter.
"""
from __future__ import annotations

import json
import logging
import os
from typing import Optional

from openai import OpenAI

from botnim.sanity.types import (
    ABVerdict,
    Answer,
    CaptureRow,
    JudgedRow,
    RubricVerdict,
    SideCapture,
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
                "verdict": {"type": "string", "enum": ["PASS_T1", "PASS_T2", "FAIL", "XFAIL"]},
                "reason": {"type": "string"},
            },
            "required": ["score", "verdict", "reason"],
            "additionalProperties": False,
        },
        "strict": True,
    },
}


def _client() -> OpenAI:
    # botnim tasks store the OpenAI key under env-suffixed names
    # (OPENAI_API_KEY_PRODUCTION / OPENAI_API_KEY_STAGING) rather than
    # the SDK-default OPENAI_API_KEY. Same convention used by the
    # embedder — see botnim.vector_store.vector_store_aurora._client.
    env = os.environ.get("ENVIRONMENT", "staging")
    api_key = (
        os.environ.get("OPENAI_API_KEY_PRODUCTION")
        if env == "production"
        else os.environ.get("OPENAI_API_KEY_STAGING")
    )
    # Final fallback to the SDK-default for local dev convenience.
    api_key = api_key or os.environ.get("OPENAI_API_KEY")
    return OpenAI(api_key=api_key)


def _side_text(side: SideCapture) -> str:
    """Format a side's turn-1 (and turn-2) into a single block for the judge."""
    parts: list[str] = []
    parts.append(f"-- TURN 1 (ok={side.turn1.ok}, error={side.turn1.error or 'none'}) --")
    parts.append(side.turn1.text or "(empty)")
    if side.turn2:
        parts.append("")
        parts.append(f"-- TURN 2 — follow-up (ok={side.turn2.ok}, error={side.turn2.error or 'none'}) --")
        parts.append(side.turn2.text or "(empty)")
    return "\n".join(parts)


def _side_has_any_text(side: SideCapture) -> bool:
    if side.turn1.ok and side.turn1.text.strip():
        return True
    if side.turn2 and side.turn2.ok and side.turn2.text.strip():
        return True
    return False


def judge_ab(row: CaptureRow) -> ABVerdict:
    new_has = _side_has_any_text(row.answer_new)
    old_has = _side_has_any_text(row.answer_old)
    if not new_has and old_has:
        err = row.answer_new.turn1.error or "no answer"
        return ABVerdict(verdict="OLD", reason=f"NEW failed to respond — {err}")
    if not old_has and new_has:
        return ABVerdict(verdict="NEW", reason="OLD failed to respond — NEW returned a real answer.")
    if not new_has and not old_has:
        return ABVerdict(verdict="TIE", reason="Both bots failed to respond.")

    followup_block = (
        f"Follow-up prompt (sent on the same conversation as turn 2):\n{row.followup_prompt}\n\n"
        if row.followup_prompt
        else "No follow-up was sent (entry has no `followup_prompt`).\n\n"
    )
    after_followup_block = (
        f"Expected after follow-up (the safety-net bar for turn 2):\n{row.expected_after_followup}\n\n"
        if row.expected_after_followup
        else ""
    )

    user = (
        f"Question (Hebrew, turn 1):\n{row.question}\n\n"
        f"Expected behavior — IDEAL ONE-TURN ANSWER (the bar for turn 1):\n{row.expected_behavior}\n\n"
        f"{followup_block}{after_followup_block}"
        f"OLD bot — captured turns:\n{_side_text(row.answer_old)}\n\n"
        f"NEW bot — captured turns:\n{_side_text(row.answer_new)}\n\n"
        "Decide which bot did better OVERALL across the captured turns. Consider:\n"
        "  1) Which turn 1 was closer to `expected_behavior` (full single-turn answer).\n"
        "  2) If both fell short, which turn 2 was closer to `expected_after_followup`.\n"
        "  3) Honesty about gaps — admitting 'I don't have access to חוק X' beats confidently rambling.\n"
        "NEW wins / OLD wins / TIE (sparingly). Reasons must be specific and short, "
        "naming what the loser got wrong (cite turn 1 or turn 2 explicitly when relevant)."
    )
    return _call_and_parse(user_msg=user, schema=_AB_SCHEMA, parser=_parse_ab)


def judge_rubric(row: CaptureRow) -> RubricVerdict:
    # Turn-1 INFRA short-circuit — no point asking the judge if NEW didn't
    # produce ANY answer at all.
    if not row.answer_new.turn1.ok and not (row.answer_new.turn1.text or "").strip():
        # If turn 2 also has nothing, this row is purely infra-failed.
        if not row.answer_new.turn2 or (
            not row.answer_new.turn2.ok and not (row.answer_new.turn2.text or "").strip()
        ):
            err = row.answer_new.turn1.error or "no answer"
            return RubricVerdict(score=None, verdict="INFRA", reason=err)

    followup_block = (
        f"Follow-up prompt sent on the same conversation:\n{row.followup_prompt}\n\n"
        if row.followup_prompt
        else "No follow-up was sent for this row.\n\n"
    )
    after_followup_block = (
        f"Expected after follow-up (the SAFETY-NET bar — earns PASS_T2 if turn 1 fell short):\n{row.expected_after_followup}\n\n"
        if row.expected_after_followup
        else "No `expected_after_followup` configured — turn 2 cannot earn PASS_T2 for this row.\n\n"
    )

    user = (
        f"Question (Hebrew, turn 1):\n{row.question}\n\n"
        f"Expected behavior — IDEAL ONE-TURN ANSWER (the PASS_T1 bar):\n{row.expected_behavior}\n\n"
        f"{followup_block}{after_followup_block}"
        f"Must NOT contain (substrings that signal a known wrong-answer mode):\n{row.must_not_contain}\n\n"
        f"Observed notes (Hebrew commentary from goldset author):\n{row.observed_notes}\n\n"
        f"NEW bot — captured turns to score:\n{_side_text(row.answer_new)}\n\n"
        "Verdicts (single label):\n"
        "  PASS_T1 — turn 1 alone covered `expected_behavior` completely. (score 0.85–1.0)\n"
        "  PASS_T2 — turn 1 fell short BUT turn 2 satisfied `expected_after_followup`. (score 0.6–0.84)\n"
        "  FAIL    — neither turn produced an adequate answer. (score 0.0–0.59)\n"
        "  XFAIL   — documented corpus gap (e.g., row 7's ועדת חקירה ממלכתית when admitting "
        "no access to חוק ועדות חקירה; row 10's חוק חובת המכרזים not retrievable; row 4's "
        "missing support amounts even after follow-up). Score reflects what you saw.\n"
        "Hebrew variants (niqqud, ועדת/וועדת, synonyms) do NOT lose points. "
        "If the answer triggers a `must_not_contain` substring, drop ≥0.4 and call it out. "
        "The `reason` MUST name what turn 1 covered/missed and (if a follow-up was sent) "
        "what turn 2 added or didn't add."
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
    except Exception as e:  # noqa: BLE001
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
