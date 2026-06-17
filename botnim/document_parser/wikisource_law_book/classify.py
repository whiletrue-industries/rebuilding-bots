"""Classify a WikiSource page title as primary law, secondary regulation, or noise.

Classification by leading token is deliberate: it doubles as the noise filter
(navigation / Help / project pages never start with a statute keyword) and
needs no network call.
"""

# Primary legislation
_LAW_PREFIXES = ("חוק ", "חוק-יסוד", "חוק יסוד", "פקודת ", "פקודה ")
# Secondary legislation
_REGULATION_PREFIXES = ("תקנות ", "צו ", "צווי ", "כללי ", "כללים ", "הוראות ", "תקנון ")


def classify_title(title: str) -> str:
    t = (title or "").strip()
    for p in _LAW_PREFIXES:
        if t.startswith(p):
            return "law"
    for p in _REGULATION_PREFIXES:
        if t.startswith(p):
            return "regulation"
    return "other"
