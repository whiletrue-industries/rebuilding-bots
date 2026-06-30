"""Classify a WikiSource page title as primary law, secondary regulation, or noise.

Classification by leading token is deliberate: it doubles as the noise filter
(navigation / Help / project pages never start with a statute keyword) and
needs no network call.
"""

# Primary legislation
_LAW_PREFIXES = ("חוק ", "חוק-יסוד", "חוק יסוד", "פקודת ", "פקודה ")
# Secondary legislation
_REGULATION_PREFIXES = ("תקנות ", "צו ", "צווי ", "כללי ", "כללים ", "הוראות ", "תקנון ")
# Carry-over items from the legal_text context that classify as "other" by
# leading token (they start with החלט…) but must be ingested into israeli_laws
# for the single-source consolidation. Kept as an explicit, narrow allow-list —
# we deliberately do NOT broaden to all החלטות/הנחיות, which would pull in
# government decisions and other noise.
_CARRYOVER_REGULATION_PREFIXES = ("החלטת שכר חברי הכנסת",)


def classify_title(title: str) -> str:
    t = (title or "").strip()
    for p in _LAW_PREFIXES:
        if t.startswith(p):
            return "law"
    for p in _REGULATION_PREFIXES:
        if t.startswith(p):
            return "regulation"
    for p in _CARRYOVER_REGULATION_PREFIXES:
        if t.startswith(p):
            return "regulation"
    return "other"
