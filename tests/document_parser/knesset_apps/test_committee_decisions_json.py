"""Unit tests for knesset_apps.committee_decisions_json."""
from __future__ import annotations

import csv
import io
import pytest
from pathlib import Path
from unittest.mock import MagicMock

from botnim.document_parser.knesset_apps.committee_decisions_json import (
    CommitteeDecisionsConfig,
    DEFAULT_KNESSET_COMMITTEE_ID,
    _decrement_one_second,
    _knesset_num_from_ids,
    _to_api_datetime,
    fetch_committee_decisions_index,
)
from botnim.document_parser.knesset_apps.common import (
    DocRow,
    EmptyUpstreamIndex,
    atomic_write_csv,
)
from botnim.storage.local_fs import LocalFsStore

_KEY = "cache/unified/extraction/committee_decisions.csv"


def _resp(payload, status=200):
    r = MagicMock()
    r.status_code = status
    r.json.return_value = payload
    r.raise_for_status = MagicMock()
    return r


# ---------- helpers ----------

def test_to_api_datetime_pads_date_only_strings():
    assert _to_api_datetime("2024-05-12") == "2024-05-12T00:00:00"


def test_to_api_datetime_passes_full_iso_through():
    assert _to_api_datetime("2024-05-12T08:30:00") == "2024-05-12T08:30:00"


def test_to_api_datetime_passes_none_through():
    assert _to_api_datetime(None) is None


def test_decrement_one_second():
    assert _decrement_one_second("2023-01-09T09:30:00") == "2023-01-09T09:29:59"
    assert _decrement_one_second("2023-01-09T09:30:30") == "2023-01-09T09:30:29"
    # Crosses minute boundary:
    assert _decrement_one_second("2023-01-09T09:30:00") == "2023-01-09T09:29:59"


def test_knesset_num_from_ids_picks_max():
    assert _knesset_num_from_ids("25") == 25
    assert _knesset_num_from_ids("24,25") == 25
    assert _knesset_num_from_ids("23, 25, 24") == 25


def test_default_committee_id_is_knesset_committee():
    """Sanity: 2211 is ועדת הכנסת — the unified bot's default scope."""
    assert DEFAULT_KNESSET_COMMITTEE_ID == 2211


# ---------- single-page paths ----------

def test_fetch_one_page_writes_index_csv(tmp_path: Path):
    store = LocalFsStore(tmp_path)
    cfg = CommitteeDecisionsConfig(store=store, key=_KEY, from_date="2022-11-15")
    http = MagicMock(side_effect=[
        _resp({
            "TotalItems": 2,
            "Items": [
                {"ItemId": 1, "DecisionDate": "2024-06-15T10:00:00",
                 "DocumentTitle": "Decision A", "DocumentPath": "https://fs/a.pdf",
                 "FileFormat": "pdf"},
                {"ItemId": 2, "DecisionDate": "2024-06-10T10:00:00",
                 "DocumentTitle": "Decision B", "DocumentPath": "https://fs/b.pdf",
                 "FileFormat": "pdf"},
            ],
        }),
        # iter 1: same items returned (server's "<= ToDate" semantics) — the
        # cursor loop sees no new items and stops.
        _resp({"TotalItems": 2, "Items": []}),
    ])
    rows = fetch_committee_decisions_index(cfg, http_post=http)
    assert len(rows) == 2
    assert {r.url for r in rows} == {"https://fs/a.pdf", "https://fs/b.pdf"}
    assert store.exists(_KEY)
    loaded = list(csv.DictReader(io.StringIO(store.get_bytes(_KEY).decode("utf-8"))))
    assert len(loaded) == 2
    assert loaded[0]["title"] == "Decision A"
    assert loaded[0]["filename"] == "1.pdf"
    assert loaded[0]["knesset_num"] == "25"


def test_fetch_filters_non_pdf_FileFormat(tmp_path: Path):
    store = LocalFsStore(tmp_path)
    cfg = CommitteeDecisionsConfig(store=store, key=_KEY)
    http = MagicMock(side_effect=[
        _resp({"Items": [
            {"ItemId": 1, "DecisionDate": "2024-01-01T00:00:00",
             "DocumentTitle": "T", "DocumentPath": "https://fs/a.pdf",
             "FileFormat": "pdf"},
            {"ItemId": 2, "DecisionDate": "2024-01-02T00:00:00",
             "DocumentTitle": "T", "DocumentPath": "https://fs/b.docx",
             "FileFormat": "doc"},
        ]}),
        _resp({"Items": []}),
    ])
    rows = fetch_committee_decisions_index(cfg, http_post=http)
    assert len(rows) == 1
    assert rows[0].url == "https://fs/a.pdf"


def test_fetch_normalizes_backslashes_in_url(tmp_path: Path):
    """DocumentPath comes back with Windows-style backslashes."""
    store = LocalFsStore(tmp_path)
    cfg = CommitteeDecisionsConfig(store=store, key=_KEY)
    http = MagicMock(side_effect=[
        _resp({"Items": [
            {"ItemId": 1, "DecisionDate": "2024-01-01T00:00:00",
             "DocumentTitle": "T",
             "DocumentPath": "https://fs.knesset.gov.il/25\\Committees\\foo.pdf",
             "FileFormat": "pdf"},
        ]}),
        _resp({"Items": []}),
    ])
    rows = fetch_committee_decisions_index(cfg, http_post=http)
    assert rows[0].url == "https://fs.knesset.gov.il/25/Committees/foo.pdf"


# ---------- pagination ----------

def test_fetch_paginates_via_to_date_cursor(tmp_path: Path):
    """Server returns 10 rows / call regardless of any PageNum/PageSize.
    We must call repeatedly with ToDate = oldest_seen - 1s until no new
    items return."""
    store = LocalFsStore(tmp_path)
    cfg = CommitteeDecisionsConfig(store=store, key=_KEY)

    page1 = [
        {"ItemId": i, "DecisionDate": f"2024-0{i}-15T10:00:00",
         "DocumentTitle": f"D{i}", "DocumentPath": f"https://fs/{i}.pdf",
         "FileFormat": "pdf"}
        for i in range(9, 0, -1)  # 9..1, descending
    ]
    page2 = [
        {"ItemId": 10, "DecisionDate": "2023-12-15T10:00:00",
         "DocumentTitle": "D10", "DocumentPath": "https://fs/10.pdf",
         "FileFormat": "pdf"},
        # And the 4 oldest happen to share the same instant — exercise
        # the cursor-collision case.
        {"ItemId": 11, "DecisionDate": "2023-01-09T09:30:00",
         "DocumentTitle": "D11", "DocumentPath": "https://fs/11.pdf",
         "FileFormat": "pdf"},
    ]

    http = MagicMock(side_effect=[
        _resp({"TotalItems": 11, "Items": page1}),
        _resp({"TotalItems": 11, "Items": page2}),
        _resp({"Items": []}),  # exhausted
    ])
    rows = fetch_committee_decisions_index(cfg, http_post=http)
    ids_seen = {int(r.filename.split(".")[0]) for r in rows}
    assert ids_seen == set(range(1, 12))

    # Verify cursor decrement logic: second call should have ToDate
    # = oldest of page1 (Item 1, "2024-01-15T10:00:00") - 1s.
    second_call_body = http.call_args_list[1].kwargs["json"]
    assert second_call_body["ToDate"] == "2024-01-15T09:59:59"


def test_fetch_collects_all_urls_when_meeting_spans_pages(tmp_path: Path):
    """Regression: ItemId is a *meeting* id, not a decision id. One meeting
    can have multiple decisions (distinct DocumentPath URLs) — verified
    live: ItemId 2242119 has 4 distinct URLs. Combined with the API's
    date-level ToDate truncation (verified live: ToDate=YYYY-MM-DDThh:mm:ss
    returns same-DAY items regardless of time), the same meeting can
    appear on multiple pages with *different* docs each time.

    Old bug: the loop filtered rows by ``ItemId not in seen_item_ids``
    BEFORE inspecting URLs, so any decision document of an already-seen
    meeting was silently dropped. Live impact (committee 2211, knesset 25,
    from_date=2022-11-15): server has 89 distinct PDF URLs, fetcher
    collected 85 — 4 lost to this bug.
    """
    store = LocalFsStore(tmp_path)
    cfg = CommitteeDecisionsConfig(store=store, key=_KEY)

    # Page 1: meeting M (ItemId=200) has 1 of its 3 decisions on this page,
    # plus 2 newer items at meeting M_NEW (ItemId=100, 2 decisions).
    page1 = [
        {"ItemId": 100, "DecisionDate": "2024-06-15T10:00:00",
         "DocumentTitle": "X1", "DocumentPath": "https://fs/m1.pdf",
         "FileFormat": "pdf"},
        {"ItemId": 100, "DecisionDate": "2024-06-15T10:00:00",
         "DocumentTitle": "X2", "DocumentPath": "https://fs/m2.pdf",
         "FileFormat": "pdf"},
        {"ItemId": 200, "DecisionDate": "2024-05-20T12:15:00",
         "DocumentTitle": "Y1", "DocumentPath": "https://fs/o1.pdf",
         "FileFormat": "pdf"},
    ]
    # Cursor advances to 2024-05-20T12:14:59. The API treats ToDate as
    # date-level (verified live), so it returns 2024-05-20 items again,
    # this time surfacing meeting 200's *other* 2 decisions, plus an
    # older filler at meeting 300.
    page2 = [
        {"ItemId": 200, "DecisionDate": "2024-05-20T12:15:00",
         "DocumentTitle": "Y2", "DocumentPath": "https://fs/o2.pdf",
         "FileFormat": "pdf"},
        {"ItemId": 200, "DecisionDate": "2024-05-20T12:15:00",
         "DocumentTitle": "Y3", "DocumentPath": "https://fs/o3.pdf",
         "FileFormat": "pdf"},
        {"ItemId": 300, "DecisionDate": "2024-04-01T10:00:00",
         "DocumentTitle": "Z", "DocumentPath": "https://fs/z.pdf",
         "FileFormat": "pdf"},
    ]
    http = MagicMock(side_effect=[
        _resp({"TotalItems": 6, "Items": page1}),
        _resp({"TotalItems": 4, "Items": page2}),
        _resp({"Items": []}),  # exhausted
    ])
    rows = fetch_committee_decisions_index(cfg, http_post=http)

    # All 6 distinct URLs must be collected, including o2/o3 that share
    # an already-seen ItemId with o1.
    assert {r.url for r in rows} == {
        "https://fs/m1.pdf",
        "https://fs/m2.pdf",
        "https://fs/o1.pdf",
        "https://fs/o2.pdf",
        "https://fs/o3.pdf",
        "https://fs/z.pdf",
    }
    assert len(rows) == 6


def test_fetch_stops_when_no_new_items(tmp_path: Path):
    store = LocalFsStore(tmp_path)
    cfg = CommitteeDecisionsConfig(store=store, key=_KEY)
    http = MagicMock(side_effect=[
        _resp({"Items": [
            {"ItemId": 1, "DecisionDate": "2024-01-01T00:00:00",
             "DocumentTitle": "T", "DocumentPath": "https://fs/a.pdf",
             "FileFormat": "pdf"},
        ]}),
        # iter 1: server returns the SAME item (boundary tie). Loop
        # detects no new items and stops.
        _resp({"Items": [
            {"ItemId": 1, "DecisionDate": "2024-01-01T00:00:00",
             "DocumentTitle": "T", "DocumentPath": "https://fs/a.pdf",
             "FileFormat": "pdf"},
        ]}),
    ])
    rows = fetch_committee_decisions_index(cfg, http_post=http)
    assert len(rows) == 1
    assert http.call_count == 2


def test_fetch_respects_max_iterations_safety_cap(tmp_path: Path):
    """If the API misbehaves and never returns new=0, we cap iterations."""
    store = LocalFsStore(tmp_path)
    cfg = CommitteeDecisionsConfig(store=store, key=_KEY, max_iterations=3)
    items = lambda i: [{
        "ItemId": i,
        "DecisionDate": f"2024-0{i}-15T10:00:00",
        "DocumentTitle": f"D{i}",
        "DocumentPath": f"https://fs/{i}.pdf",
        "FileFormat": "pdf",
    }]
    http = MagicMock(side_effect=[
        _resp({"Items": items(3)}),
        _resp({"Items": items(2)}),
        _resp({"Items": items(1)}),
        # If max_iterations weren't honored, we'd run out of mock responses.
    ])
    rows = fetch_committee_decisions_index(cfg, http_post=http)
    assert {r.filename for r in rows} == {"1.pdf", "2.pdf", "3.pdf"}
    assert http.call_count == 3


# ---------- empty-result safety guard ----------

def test_empty_with_existing_csv_raises(tmp_path: Path):
    store = LocalFsStore(tmp_path)
    atomic_write_csv(store, _KEY, [DocRow(url="u", filename="f", date="d", knesset_num=1)])
    cfg = CommitteeDecisionsConfig(store=store, key=_KEY)
    http = MagicMock(return_value=_resp({"TotalItems": 0, "Items": []}))
    with pytest.raises(EmptyUpstreamIndex):
        fetch_committee_decisions_index(cfg, http_post=http)
    # Store object untouched
    loaded = list(csv.DictReader(io.StringIO(store.get_bytes(_KEY).decode("utf-8"))))
    assert len(loaded) == 1


def test_empty_first_run_is_allowed(tmp_path: Path):
    store = LocalFsStore(tmp_path)
    cfg = CommitteeDecisionsConfig(store=store, key=_KEY)
    http = MagicMock(return_value=_resp({"Items": []}))
    rows = fetch_committee_decisions_index(cfg, http_post=http)
    assert rows == []
    assert store.exists(_KEY)
    loaded = list(csv.DictReader(io.StringIO(store.get_bytes(_KEY).decode("utf-8"))))
    assert loaded == []


# ---------- request-body shape ----------

def test_request_body_matches_live_api_contract(tmp_path: Path):
    """Mirror the exact JSON shape we sniffed from the SharePoint page —
    if the API ever rejects our shape, this test should fail next time."""
    store = LocalFsStore(tmp_path)
    cfg = CommitteeDecisionsConfig(
        store=store,
        key=_KEY,
        committee_id=2211,
        from_date="2022-11-15",
        knesset_ids="25",
    )
    http = MagicMock(return_value=_resp({"Items": []}))
    fetch_committee_decisions_index(cfg, http_post=http)
    body = http.call_args.kwargs["json"]
    assert body == {
        "CommitteeId": 2211,
        "FromDate": "2022-11-15T00:00:00",
        "ToDate": None,
        "KnessetIDs": "25",
        "Language": "he",
        "Subject": "",
    }
