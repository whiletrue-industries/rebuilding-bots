"""LLM categorization into Tal's controlled vocab.

One ``client.chat.completions.create`` call per (uncached) decision,
gpt-4o-mini, ``response_format={"type": "json_object"}``, temperature 0.
At ~5–50 new decisions/week post-bootstrap that's well under $0.05 per
refresh. The vocab below is the exact label set used in
Gov_Res_fulldata_25032026.xlsx — preserving it lets us mix LLM-labeled
new rows with bootstrap rows without taxonomy drift.

Failure handling: out-of-vocab → one stricter retry → fallback to
('אחר', 'כללי'). We never raise from this module; the orchestrator
wants to keep going if 1 of 50 new decisions can't be categorized.
"""
from __future__ import annotations

import json
import logging
from typing import Final

from ...config import get_logger, get_openai_client

logger: logging.Logger = get_logger(__name__)


ACTION_TYPES: Final[tuple[str, ...]] = (
    "אחר",
    "דוחות ועדות",
    "הכרזות וצווים",
    "הסכמים",
    "הקמת גופים",
    "חקיקה",
    "יישום ומעקב",
    "כלכלה ותעשייה",
    "מדיניות",
    "מינויים",
    "נסיעות",
    "עבודה ותעסוקה",
    "תכנון ובנייה",
    "תקציב וכספים",
    "תרבות וספורט",
    "תשתיות ותחבורה",
)

DOMAINS: Final[tuple[str, ...]] = (
    "ביטחון וצבא",
    "בריאות",
    "דת",
    "התיישבות ופריפריה",
    "חברה ערבית ומיעוטים",
    "חוץ ודיפלומטיה",
    "חינוך ומדע",
    "כלכלה ותעשייה",
    "כללי",
    "משפט ואכיפה",
    "סביבה ואקלים",
    "עבודה ותעסוקה",
    "פנים ושלטון מקומי",
    "רווחה וחברה",
    "שיכון וקרקעות",
    "תיירות",
    "תכנון ובנייה",
    "תקציב וכספים",
    "תקשורת",
    "תקשורת ומדיה",
    "תרבות וספורט",
    "תשתיות ותחבורה",
)


_FALLBACK = {"action_type": "אחר", "domain": "כללי"}
_BODY_CHAR_LIMIT = 6000  # cap context to keep token use bounded


def _system_prompt() -> str:
    at = "\n".join(f"- {x}" for x in ACTION_TYPES)
    dm = "\n".join(f"- {x}" for x in DOMAINS)
    return (
        "אתה מסווג החלטות ממשלה ישראליות לפי שני שדות: 'סוג פעולה' ו'תחום'.\n"
        "החזר JSON תקין בלבד עם המפתחות action_type ו-domain.\n\n"
        "ערכים מותרים עבור action_type (בחר *בדיוק* אחד):\n"
        f"{at}\n\n"
        "ערכים מותרים עבור domain (בחר *בדיוק* אחד):\n"
        f"{dm}\n\n"
        "אם לא ברור, השתמש ב-'אחר' עבור action_type ו-'כללי' עבור domain."
    )


def _user_prompt(title: str, text: str) -> str:
    body = (text or "")[:_BODY_CHAR_LIMIT]
    return f"כותרת:\n{title}\n\nגוף ההחלטה:\n{body}"


def _parse_and_validate(raw: str) -> dict | None:
    try:
        obj = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return None
    at = obj.get("action_type")
    dm = obj.get("domain")
    if at in ACTION_TYPES and dm in DOMAINS:
        return {"action_type": at, "domain": dm}
    return None


def categorize(*, title: str, text: str) -> dict:
    """Return ``{"action_type": ..., "domain": ...}`` from the controlled vocab.

    Always returns a dict (no exceptions escape). Falls back to
    ``("אחר", "כללי")`` after one failed retry.
    """
    client = get_openai_client()
    sys_msg = _system_prompt()
    user_msg = _user_prompt(title, text)

    for attempt in (1, 2):
        try:
            resp = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": sys_msg},
                    {"role": "user", "content": user_msg},
                ],
                temperature=0.0,
                max_tokens=120,
                response_format={"type": "json_object"},
            )
            raw = resp.choices[0].message.content or ""
        except Exception as exc:
            logger.warning("categorize attempt %d failed: %s", attempt, exc)
            continue

        parsed = _parse_and_validate(raw)
        if parsed is not None:
            return parsed
        logger.warning(
            "categorize attempt %d returned out-of-vocab/malformed payload: %r",
            attempt,
            raw[:200],
        )

    logger.warning("categorize falling back to (%s, %s) for title=%r",
                   _FALLBACK["action_type"], _FALLBACK["domain"], title[:80])
    return dict(_FALLBACK)
