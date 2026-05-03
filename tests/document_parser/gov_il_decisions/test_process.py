"""Tests for the gov_il_decisions process orchestrator.

All external IO (gov.il API, Aurora) is mocked. The pytest_postgresql
fixture is used in test_aurora_writer.py for the real-DB end-to-end of
the writer; here we keep things light and focused on orchestration
flow.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest


_LISTING_ITEM_TEMPLATE = {
    "url": "https://www.gov.il/he/departments/policies/dec-NEW",
    "title": "החלטה חדשה",
    "tags": {
        "metaData": {
            "ממשלה": [{"title": "הממשלה ה- 37, בנימין נתניהו"}],
            "תאריך פרסום": [{"title": "01.04.2026"}],
            "תאריך תחולה": [{"title": "02.04.2026"}],
            "משרד": [{"title": "ראש הממשלה"}],
        },
        "promotedMetaData": {
            "מספר החלטה": [{"title": "1234"}],
        },
    },
}


def _make_listing_item(page_id: str) -> dict:
    item = {
        "url": f"https://www.gov.il/he/departments/policies/{page_id}",
        "title": f"כותרת {page_id}",
        "tags": _LISTING_ITEM_TEMPLATE["tags"],
    }
    return item


def _make_content_payload() -> dict:
    return {
        "contentMain": {
            "htmlContents": [
                {"sectionData": "<p>גוף ההחלטה</p>"},
            ],
        },
        "contentSub": {
            "filesToDownload": {"filesGroupItems": []},
        },
    }


@pytest.fixture
def mocked_writers():
    """Patch aurora_writer functions used by process.py."""
    with patch("botnim.document_parser.gov_il_decisions.process.get_or_create_context") as goc, \
         patch("botnim.document_parser.gov_il_decisions.process.existing_page_ids") as ex, \
         patch("botnim.document_parser.gov_il_decisions.process.write_decision") as wd:
        goc.return_value = "00000000-0000-0000-0000-000000000001"
        ex.return_value = set()
        wd.return_value = 1
        yield {"goc": goc, "existing": ex, "write": wd}


@pytest.fixture
def mocked_categorize():
    with patch("botnim.document_parser.gov_il_decisions.process.categorize") as c:
        c.return_value = {"action_type": "מדיניות", "domain": "כללי"}
        yield c


@pytest.fixture
def mocked_client_class():
    """Patch GovIlClient. Tests configure ``.return_value`` per case."""
    with patch("botnim.document_parser.gov_il_decisions.process.GovIlClient") as cls:
        instance = MagicMock()
        cls.return_value = instance
        yield instance


def test_skips_existing_page_ids(mocked_writers, mocked_categorize, mocked_client_class):
    from botnim.document_parser.gov_il_decisions.process import (
        process_gov_il_decisions_source,
    )

    # Pre-existing page_id; one new in listing
    mocked_writers["existing"].return_value = {"dec-OLD"}
    mocked_client_class.list_decisions.side_effect = [
        {"total": 2, "results": [_make_listing_item("dec-OLD"), _make_listing_item("dec-NEW")]},
        {"total": 2, "results": []},
    ]
    mocked_client_class.fetch_content.return_value = _make_content_payload()

    process_gov_il_decisions_source(environment="staging", page_size=50, max_pages=2)

    # fetch_content + categorize + write_decision called only for dec-NEW
    assert mocked_client_class.fetch_content.call_count == 1
    args, _ = mocked_client_class.fetch_content.call_args
    assert args[0] == "dec-NEW"
    assert mocked_categorize.call_count == 1
    assert mocked_writers["write"].call_count == 1


def test_404_on_content_skips_page_id(mocked_writers, mocked_categorize, mocked_client_class):
    from botnim.document_parser.gov_il_decisions.process import (
        process_gov_il_decisions_source,
    )

    mocked_client_class.list_decisions.side_effect = [
        {"total": 1, "results": [_make_listing_item("dec-GONE")]},
        {"total": 1, "results": []},
    ]
    mocked_client_class.fetch_content.return_value = None  # 404

    process_gov_il_decisions_source(environment="staging", page_size=50, max_pages=2)

    assert mocked_writers["write"].call_count == 0


def test_empty_listing_and_empty_context_raises(mocked_writers, mocked_categorize, mocked_client_class):
    from botnim.document_parser.gov_il_decisions.exceptions import EmptyUpstreamIndex
    from botnim.document_parser.gov_il_decisions.process import (
        process_gov_il_decisions_source,
    )

    mocked_writers["existing"].return_value = set()
    mocked_client_class.list_decisions.return_value = {"total": 0, "results": []}

    with pytest.raises(EmptyUpstreamIndex):
        process_gov_il_decisions_source(environment="staging", page_size=50, max_pages=2)


def test_decision_metadata_shape(mocked_writers, mocked_categorize, mocked_client_class):
    from botnim.document_parser.gov_il_decisions.process import (
        process_gov_il_decisions_source,
    )

    mocked_client_class.list_decisions.side_effect = [
        {"total": 1, "results": [_make_listing_item("dec-NEW")]},
        {"total": 1, "results": []},
    ]
    mocked_client_class.fetch_content.return_value = _make_content_payload()

    process_gov_il_decisions_source(environment="staging", page_size=50, max_pages=2)

    assert mocked_writers["write"].call_count == 1
    _, kwargs = mocked_writers["write"].call_args
    md = kwargs["metadata"]
    assert kwargs["page_id"] == "dec-NEW"
    assert kwargs["environment"] == "staging"
    assert md["page_id"] == "dec-NEW"
    assert md["action_type"] == "מדיניות"
    assert md["domain"] == "כללי"
    assert md["source_url"] == "https://www.gov.il/he/departments/policies/dec-NEW"
    assert md["decision_number"] == "1234"
    assert md["government_number"] == "37"
    assert md["government"] == "הממשלה ה- 37, בנימין נתניהו"
    assert md["publish_date"] == "01.04.2026"
    assert md["effective_date"] == "02.04.2026"
    assert md["office"] == "ראש הממשלה"
    assert md["has_attachment"] is False
    assert md["attachment_urls"] == []
