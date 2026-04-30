"""Unit tests for the Knesset OData fetcher.

Covers:
- Happy path: sessions + items are fetched and joined into one CSV row per
  (session, item) pair, with session metadata duplicated.
- Empty agenda: a session with zero items still emits exactly one row.
- Empty upstream: zero sessions raises EmptyUpstreamIndex without touching
  the output CSV.
- Hash short-circuit: re-running with the same upstream data leaves the
  output untouched.
- Date filter formatting: the OData $filter literal uses the
  ``datetime'YYYY-MM-DDTHH:MM:SS'`` form expected by the Knesset service.

All network calls are mocked via ``unittest.mock.patch`` on
``requests.get`` — no real HTTP traffic.
"""
from __future__ import annotations

import csv
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from botnim.document_parser.knesset_odata import process_odata
from botnim.document_parser.pdfs.exceptions import EmptyUpstreamIndex


def _json_response(payload: dict) -> MagicMock:
    resp = MagicMock()
    resp.json.return_value = payload
    resp.raise_for_status = MagicMock()
    return resp


def _sessions_payload(rows: list[dict]) -> dict:
    return {"value": rows}


def _items_payload(rows: list[dict]) -> dict:
    return {"value": rows}


def _make_session(
    sid: int,
    *,
    name: str = "ישיבת מליאה",
    start: str = "2026-05-04T11:00:00",
    finish: str = "2026-05-04T15:00:00",
    knesset: int = 25,
    number: int = 100,
    special: bool = False,
    last_updated: str = "2026-05-01T08:00:00",
) -> dict:
    return {
        "PlenumSessionID": sid,
        "Number": number,
        "KnessetNum": knesset,
        "Name": name,
        "StartDate": start,
        "FinishDate": finish,
        "IsSpecialMeeting": special,
        "LastUpdatedDate": last_updated,
    }


def _make_item(
    pid: int,
    *,
    sid: int,
    ordinal: int = 1,
    item_type: str = "הצעת חוק",
    name: str = "חוק לדוגמה",
    status: int = 5,
    is_disc: int = 1,
    last_updated: str = "2026-05-01T08:00:00",
) -> dict:
    return {
        "plmPlenumSessionID": pid,
        "ItemID": pid,
        "PlenumSessionID": sid,
        "ItemTypeID": 1,
        "ItemTypeDesc": item_type,
        "Ordinal": ordinal,
        "Name": name,
        "StatusID": status,
        "IsDiscussion": is_disc,
        "LastUpdatedDate": last_updated,
    }


@pytest.fixture
def fixed_now() -> datetime:
    return datetime(2026, 5, 1, 12, 0, 0)


@patch.object(process_odata, "requests")
def test_happy_path_writes_joined_csv(mock_requests, tmp_path: Path, fixed_now):
    """Two sessions × two items + one empty session → 5 rows total."""
    sessions = [
        _make_session(1001, name="ישיבת מליאה 1", start="2026-05-04T11:00:00"),
        _make_session(1002, name="ישיבת מליאה 2", start="2026-05-05T16:00:00"),
        _make_session(1003, name="ישיבת מליאה ריקה", start="2026-05-06T10:00:00"),
    ]
    items = [
        _make_item(1, sid=1001, ordinal=1, name="חוק א'"),
        _make_item(2, sid=1001, ordinal=2, name="חוק ב'"),
        _make_item(3, sid=1002, ordinal=1, name="הצעה לסדר היום"),
        _make_item(4, sid=1002, ordinal=2, name="חוק ג'"),
        # No items for session 1003 → one row with empty item columns.
    ]
    mock_requests.get.side_effect = [
        _json_response(_sessions_payload(sessions)),
        _json_response(_items_payload(items)),
    ]

    out = tmp_path / "plenary_schedule.csv"
    process_odata.process_knesset_odata_source(
        output_csv_path=out,
        base_url="https://example.test/Odata/ParliamentInfo.svc",
        days_past=30,
        days_future=90,
        now=fixed_now,
    )

    assert out.exists()
    with out.open(encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    # 4 item rows + 1 empty-agenda row = 5
    assert len(rows) == 5
    # Session 1001 has two items.
    s1 = [r for r in rows if r["session_id"] == "1001"]
    assert len(s1) == 2
    assert {r["item_name"] for r in s1} == {"חוק א'", "חוק ב'"}
    # Common session metadata duplicated.
    assert all(r["session_name"] == "ישיבת מליאה 1" for r in s1)
    assert all(r["session_date"] == "2026-05-04" for r in s1)
    assert all(r["session_time"] == "11:00" for r in s1)
    assert all(r["is_special_meeting"] == "לא" for r in s1)
    # Empty agenda emits a row with empty item columns.
    s3 = [r for r in rows if r["session_id"] == "1003"]
    assert len(s3) == 1
    assert s3[0]["item_name"] == ""
    assert s3[0]["item_type"] == ""
    # All rows share the same upstream_hash.
    hashes = {r["upstream_hash"] for r in rows}
    assert len(hashes) == 1
    assert hashes.pop()  # non-empty


@patch.object(process_odata, "requests")
def test_empty_upstream_raises(mock_requests, tmp_path: Path, fixed_now):
    """No sessions returned → EmptyUpstreamIndex, output CSV not created."""
    mock_requests.get.side_effect = [_json_response(_sessions_payload([]))]
    out = tmp_path / "plenary_schedule.csv"

    with pytest.raises(EmptyUpstreamIndex):
        process_odata.process_knesset_odata_source(
            output_csv_path=out,
            base_url="https://example.test/Odata/ParliamentInfo.svc",
            now=fixed_now,
        )
    assert not out.exists()


@patch.object(process_odata, "requests")
def test_hash_short_circuit_skips_rewrite(mock_requests, tmp_path: Path, fixed_now):
    """Same upstream payload on a second run → file mtime unchanged."""
    sessions = [_make_session(1001, name="ישיבת מליאה")]
    items = [_make_item(1, sid=1001, name="חוק א'")]

    out = tmp_path / "plenary_schedule.csv"

    # First run writes the file.
    mock_requests.get.side_effect = [
        _json_response(_sessions_payload(sessions)),
        _json_response(_items_payload(items)),
    ]
    process_odata.process_knesset_odata_source(
        output_csv_path=out,
        base_url="https://example.test/Odata/ParliamentInfo.svc",
        now=fixed_now,
    )
    first_mtime = out.stat().st_mtime_ns
    first_content = out.read_bytes()

    # Second run with identical upstream data → short-circuit, no rewrite.
    mock_requests.get.side_effect = [
        _json_response(_sessions_payload(sessions)),
        _json_response(_items_payload(items)),
    ]
    process_odata.process_knesset_odata_source(
        output_csv_path=out,
        base_url="https://example.test/Odata/ParliamentInfo.svc",
        now=fixed_now,
    )
    assert out.stat().st_mtime_ns == first_mtime
    assert out.read_bytes() == first_content


@patch.object(process_odata, "requests")
def test_hash_changes_when_item_updated(mock_requests, tmp_path: Path, fixed_now):
    """LastUpdatedDate change on an item → new hash → rewrite."""
    sessions = [_make_session(1001)]
    items_v1 = [_make_item(1, sid=1001, name="v1", last_updated="2026-05-01T08:00:00")]
    items_v2 = [_make_item(1, sid=1001, name="v2", last_updated="2026-05-02T09:00:00")]

    out = tmp_path / "plenary_schedule.csv"

    mock_requests.get.side_effect = [
        _json_response(_sessions_payload(sessions)),
        _json_response(_items_payload(items_v1)),
    ]
    process_odata.process_knesset_odata_source(
        output_csv_path=out,
        base_url="https://example.test/Odata/ParliamentInfo.svc",
        now=fixed_now,
    )
    first_content = out.read_bytes()

    mock_requests.get.side_effect = [
        _json_response(_sessions_payload(sessions)),
        _json_response(_items_payload(items_v2)),
    ]
    process_odata.process_knesset_odata_source(
        output_csv_path=out,
        base_url="https://example.test/Odata/ParliamentInfo.svc",
        now=fixed_now,
    )
    assert out.read_bytes() != first_content
    with out.open(encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    assert rows[0]["item_name"] == "v2"


def test_odata_datetime_literal_format():
    """The OData v2 service expects ``datetime'YYYY-MM-DDTHH:MM:SS'`` literals."""
    dt = datetime(2026, 5, 4, 11, 0, 0)
    assert process_odata._odata_datetime(dt) == "datetime'2026-05-04T11:00:00'"


@patch.object(process_odata, "requests")
def test_session_filter_uses_window(mock_requests, tmp_path: Path, fixed_now):
    """The first GET must constrain StartDate to [now - days_past, now + days_future)."""
    sessions = [_make_session(1001)]
    mock_requests.get.side_effect = [
        _json_response(_sessions_payload(sessions)),
        _json_response(_items_payload([])),
    ]
    out = tmp_path / "plenary_schedule.csv"
    process_odata.process_knesset_odata_source(
        output_csv_path=out,
        base_url="https://example.test/Odata/ParliamentInfo.svc",
        days_past=7,
        days_future=14,
        now=fixed_now,
    )
    # First call hits KNS_PlenumSession with the date window in $filter.
    first_call = mock_requests.get.call_args_list[0]
    url = first_call.args[0]
    params = first_call.kwargs["params"]
    assert url.endswith("/KNS_PlenumSession")
    assert "StartDate ge datetime'2026-04-24T12:00:00'" in params["$filter"]
    assert "StartDate lt datetime'2026-05-15T12:00:00'" in params["$filter"]
    assert params["$format"] == "json"


@patch.object(process_odata, "requests")
def test_paged_results_followed(mock_requests, tmp_path: Path, fixed_now):
    """``@odata.nextLink`` is followed until exhausted."""
    page1 = {
        "value": [_make_session(1001)],
        "@odata.nextLink": "https://example.test/Odata/ParliamentInfo.svc/KNS_PlenumSession?skiptoken=1",
    }
    page2 = {"value": [_make_session(1002)]}
    items = {"value": [_make_item(1, sid=1001), _make_item(2, sid=1002)]}
    mock_requests.get.side_effect = [
        _json_response(page1),
        _json_response(page2),
        _json_response(items),
    ]

    out = tmp_path / "plenary_schedule.csv"
    process_odata.process_knesset_odata_source(
        output_csv_path=out,
        base_url="https://example.test/Odata/ParliamentInfo.svc",
        now=fixed_now,
    )
    with out.open(encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    assert {r["session_id"] for r in rows} == {"1001", "1002"}
