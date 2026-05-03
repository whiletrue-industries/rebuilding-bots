"""Unit tests for the protocols fetcher orchestration.

The OData listing + .doc downloads are mocked via ``unittest.mock.patch``
on ``requests`` inside ``process_protocols`` so no real HTTP traffic
fires. We focus on:

* OData filter shape (date window + GroupTypeDesc clause)
* Hash short-circuit on unchanged upstream
* EmptyUpstreamIndex when no docs come back
* CSV row schema (one row per (doc, turn) with metadata duplicated)
* Per-document failure isolation (single bad doc doesn't abort the run)
* Rate-limit + max_protocols caps
"""
from __future__ import annotations

import csv
import io
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import docx
import pytest

from botnim.document_parser.knesset_protocols import process_protocols
from botnim.document_parser.pdfs.exceptions import EmptyUpstreamIndex


def _odata_response(rows: list[dict]) -> MagicMock:
    resp = MagicMock()
    resp.json.return_value = {"value": rows}
    resp.raise_for_status = MagicMock()
    return resp


def _doc_response(blob: bytes) -> MagicMock:
    resp = MagicMock()
    resp.content = blob
    resp.raise_for_status = MagicMock()
    return resp


def _make_doc(paragraphs: list[tuple[str, str]]) -> bytes:
    d = docx.Document()
    for style, text in paragraphs:
        p = d.add_paragraph(text)
        if style:
            try:
                p.style = d.styles[style]
            except KeyError:
                d.styles.add_style(style, 1)
                p.style = d.styles[style]
    buf = io.BytesIO()
    d.save(buf)
    return buf.getvalue()


def _committee_index(doc_id: int = 1, last="2026-04-01T00:00:00") -> dict:
    return {
        "DocumentCommitteeSessionID": str(doc_id),
        "CommitteeSessionID": doc_id * 1000,
        "GroupTypeDesc": "פרוטוקול ועדה",
        "ApplicationDesc": "DOC",
        "FilePath": f"https://fs.knesset.gov.il/25/Committees/test_{doc_id}.doc",
        "LastUpdatedDate": last,
    }


def _plenum_index(doc_id: int = 100, last="2026-04-01T00:00:00") -> dict:
    return {
        "DocumentPlenumSessionID": str(doc_id),
        "PlenumSessionID": doc_id * 1000,
        "GroupTypeDesc": "דברי הכנסת",
        "ApplicationDesc": "DOC",
        "FilePath": f"https://fs.knesset.gov.il/25/Plenum/test_{doc_id}.doc",
        "LastUpdatedDate": last,
    }


@pytest.fixture
def fixed_now():
    return datetime(2026, 5, 1, 12, 0, 0)


@pytest.fixture
def sample_committee_doc():
    return _make_doc([
        (None, "מישיבת ועדת הכספים"),
        ("נושא", "<< נושא >> נושא לבדיקה"),
        ("יור", '<< יור >> היו"ר ישראל ישראלי: << יור >>'),
        (None, "טקסט יושב הראש."),
        ("דובר", "<< דובר >> פלוני (סיעה): << דובר >>"),
        (None, "טקסט הדובר."),
    ])


@patch.object(process_protocols, "requests")
def test_happy_path_writes_csv(mock_requests, tmp_path, fixed_now,
                               sample_committee_doc):
    mock_requests.get.side_effect = [
        _odata_response([_committee_index(1)]),         # committees list
        _odata_response([]),                            # plenum list (empty)
        _doc_response(sample_committee_doc),            # download committee_1
    ]
    out = tmp_path / "knesset_protocols.csv"
    process_protocols.process_knesset_protocols_source(
        output_csv_path=out, base_url="https://example.test/Odata/x.svc",
        days_history=30, max_protocols=10, rate_limit_seconds=0,
        now=fixed_now,
    )
    rows = list(csv.DictReader(out.open(encoding="utf-8")))
    assert len(rows) == 2  # one chair + one speaker
    assert rows[0]["doc_kind"] == "committee"
    assert rows[0]["doc_group_type"] == "פרוטוקול ועדה"
    assert rows[0]["committee_name"].endswith("הכספים") or "כספים" in rows[0]["committee_name"]
    assert rows[0]["agenda_item"] == "נושא לבדיקה"
    assert rows[0]["speaker_role"] == "chair"
    assert rows[0]["speaker_name"] == "ישראל ישראלי"
    assert rows[1]["speaker_role"] == "speaker"
    assert rows[1]["speaker_party"] == "סיעה"


@patch.object(process_protocols, "requests")
def test_empty_upstream_raises(mock_requests, tmp_path, fixed_now):
    mock_requests.get.side_effect = [
        _odata_response([]),
        _odata_response([]),
    ]
    out = tmp_path / "knesset_protocols.csv"
    with pytest.raises(EmptyUpstreamIndex):
        process_protocols.process_knesset_protocols_source(
            output_csv_path=out,
            base_url="https://example.test/Odata/x.svc",
            now=fixed_now, rate_limit_seconds=0,
        )
    assert not out.exists()


@patch.object(process_protocols, "requests")
def test_hash_short_circuit(mock_requests, tmp_path, fixed_now,
                            sample_committee_doc):
    mock_requests.get.side_effect = [
        _odata_response([_committee_index(1)]),
        _odata_response([]),
        _doc_response(sample_committee_doc),
    ]
    out = tmp_path / "knesset_protocols.csv"
    process_protocols.process_knesset_protocols_source(
        output_csv_path=out,
        base_url="https://example.test/Odata/x.svc",
        days_history=30, max_protocols=10, rate_limit_seconds=0,
        now=fixed_now,
    )
    first_mtime = out.stat().st_mtime_ns

    mock_requests.get.side_effect = [
        _odata_response([_committee_index(1)]),
        _odata_response([]),
        _doc_response(sample_committee_doc),
    ]
    process_protocols.process_knesset_protocols_source(
        output_csv_path=out,
        base_url="https://example.test/Odata/x.svc",
        days_history=30, max_protocols=10, rate_limit_seconds=0,
        now=fixed_now,
    )
    assert out.stat().st_mtime_ns == first_mtime


@patch.object(process_protocols, "requests")
def test_filter_uses_window_and_group_type(mock_requests, tmp_path, fixed_now,
                                           sample_committee_doc):
    mock_requests.get.side_effect = [
        _odata_response([_committee_index(1)]),
        _odata_response([]),
        _doc_response(sample_committee_doc),
    ]
    out = tmp_path / "knesset_protocols.csv"
    process_protocols.process_knesset_protocols_source(
        output_csv_path=out,
        base_url="https://example.test/Odata/x.svc",
        days_history=10,
        max_protocols=5,
        rate_limit_seconds=0,
        now=fixed_now,
    )
    # First call: committee list
    first_call = mock_requests.get.call_args_list[0]
    params = first_call.kwargs["params"]
    assert "פרוטוקול ועדה" in params["$filter"]
    assert "LastUpdatedDate ge datetime'2026-04-21T12:00:00'" in params["$filter"]
    # Second call: plenum list
    second_call = mock_requests.get.call_args_list[1]
    assert "דברי הכנסת" in second_call.kwargs["params"]["$filter"]


@patch.object(process_protocols, "requests")
def test_failed_download_does_not_abort(mock_requests, tmp_path, fixed_now,
                                        sample_committee_doc):
    bad_resp = MagicMock()
    bad_resp.raise_for_status.side_effect = RuntimeError("upstream 503")
    mock_requests.get.side_effect = [
        _odata_response([_committee_index(1), _committee_index(2)]),
        _odata_response([]),
        bad_resp,                                         # download 1 fails
        _doc_response(sample_committee_doc),              # download 2 ok
    ]
    out = tmp_path / "knesset_protocols.csv"
    process_protocols.process_knesset_protocols_source(
        output_csv_path=out,
        base_url="https://example.test/Odata/x.svc",
        days_history=30, max_protocols=10, rate_limit_seconds=0,
        now=fixed_now,
    )
    rows = list(csv.DictReader(out.open(encoding="utf-8")))
    # Should have 2 turns from doc 2 only.
    assert len(rows) == 2
    assert rows[0]["document_id"] == "2"


@patch.object(process_protocols, "requests")
def test_max_protocols_cap(mock_requests, tmp_path, fixed_now,
                           sample_committee_doc):
    """Cap should stop fetching after N committees AND skip plenum entirely."""
    mock_requests.get.side_effect = [
        _odata_response([_committee_index(i) for i in range(1, 4)]),   # 3 committees
        _doc_response(sample_committee_doc),
        _doc_response(sample_committee_doc),
        _doc_response(sample_committee_doc),
        # No plenum call expected — cap=3 already reached
    ]
    out = tmp_path / "knesset_protocols.csv"
    process_protocols.process_knesset_protocols_source(
        output_csv_path=out,
        base_url="https://example.test/Odata/x.svc",
        days_history=30, max_protocols=3, rate_limit_seconds=0,
        now=fixed_now,
    )
    # Only committee list + 3 downloads = 4 calls. No plenum list.
    assert mock_requests.get.call_count == 4


@patch.object(process_protocols, "requests")
def test_skips_non_doc_paths(mock_requests, tmp_path, fixed_now,
                             sample_committee_doc):
    """Index entries whose FilePath isn't .doc/.docx are skipped without downloading."""
    pdf_entry = _committee_index(1)
    pdf_entry["FilePath"] = "https://fs.knesset.gov.il/25/Committees/x.pdf"
    mock_requests.get.side_effect = [
        _odata_response([pdf_entry, _committee_index(2)]),
        _odata_response([]),
        _doc_response(sample_committee_doc),  # only doc 2 downloaded
    ]
    out = tmp_path / "knesset_protocols.csv"
    process_protocols.process_knesset_protocols_source(
        output_csv_path=out,
        base_url="https://example.test/Odata/x.svc",
        days_history=30, max_protocols=10, rate_limit_seconds=0,
        now=fixed_now,
    )
    rows = list(csv.DictReader(out.open(encoding="utf-8")))
    assert all(r["document_id"] == "2" for r in rows)
