"""First-party fetchers for ``main.knesset.gov.il/apps/...`` pages.

The Knesset's "apps" SharePoint pages (committee decisions per
committee, ethics decisions, etc.) are powered by JSON APIs hosted
on ``www.knesset.gov.il/WebSiteApi/knessetapi/...``. The APIs are
**not** behind the Reblaze JS challenge that gates ``main.knesset.gov.il``
HTML pages — plain ``requests`` calls work directly. This is the
cheapest, most reliable replacement for BudgetKey's frozen
``knesset_committee_decisions`` and ``ethics_committee_decisions``
datapackages.

Two fetchers in this module:

  - ``committee_decisions_json`` — POSTs to
    ``CommitteeDecisions/GetCommitteePortalsDecisions`` to get the
    decisions of one specific committee filtered by Knesset and
    date. Used for ``ועדת הכנסת`` (committee_id=2211).

  - ``ethics_decisions_html`` — GETs ``Pages/GetPage`` which returns
    the SharePoint page's pre-rendered HTML wrapped in JSON; we then
    extract the PDF anchors from that HTML. Used for ``ועדת האתיקה``
    (committee_id=2217, page=EthicsDecisions25).

Both produce the same-shape ``index.csv`` (``url, filename, date,
knesset_num``) that the existing ``kind: pdf`` downstream pipeline
(download → OCR → LLM extract → embed) consumes unchanged.
"""
from .committee_decisions_json import (
    DEFAULT_KNESSET_COMMITTEE_ID,
    CommitteeDecisionsConfig,
    fetch_committee_decisions_index,
)
from .ethics_decisions_html import (
    EthicsDecisionsConfig,
    fetch_ethics_decisions_index,
)
from .common import DocRow, EmptyUpstreamIndex

__all__ = [
    "CommitteeDecisionsConfig",
    "DEFAULT_KNESSET_COMMITTEE_ID",
    "DocRow",
    "EmptyUpstreamIndex",
    "EthicsDecisionsConfig",
    "fetch_committee_decisions_index",
    "fetch_ethics_decisions_index",
]
