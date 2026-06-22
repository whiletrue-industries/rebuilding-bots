"""Tests for URL-to-metadata promotion in collect_sources._build_metadata_record."""
from botnim.collect_sources import _build_metadata_record


def _plenary_content(session_id: int = 1234) -> str:
    return (
        f"session_id:\n{session_id}\n\n"
        f"session_date:\n2026-04-27\n\n"
        f"source_url:\n"
        f"https://www.knesset.gov.il/plenum/heb/sessionDet.aspx?SessionID={session_id}\n\n"
        f"item_name:\nחוק הביטוח הלאומי\n\n"
    )


def test_source_url_column_sets_metadata_source_url():
    """source_url column → metadata['source_url'] with full URL."""
    meta = _build_metadata_record(
        _plenary_content(1234), "plenary_schedule_0.md", "text/markdown", None, None
    )
    assert meta["source_url"] == (
        "https://www.knesset.gov.il/plenum/heb/sessionDet.aspx?SessionID=1234"
    )


def test_source_url_column_does_not_change_source_doc():
    """source_doc should still be the session_id integer string, not the URL."""
    meta = _build_metadata_record(
        _plenary_content(1234), "plenary_schedule_0.md", "text/markdown", None, None
    )
    assert meta.get("source_doc") == "1234"


def test_no_url_column_leaves_source_url_absent():
    """Docs with no URL column (most legal/lexicon contexts) get no source_url."""
    content = "session_id:\n9999\n\nitem_name:\nהצעת חוק\n\n"
    meta = _build_metadata_record(content, "test.md", "text/markdown", None, None)
    assert "source_url" not in meta


def test_file_url_column_also_sets_source_url():
    """file_url column (used by some existing CSV contexts) also triggers source_url."""
    content = (
        "document_id:\nDOC-42\n\n"
        "file_url:\nhttps://example.com/documents/42.pdf\n\n"
        "title:\nמסמך לדוגמה\n\n"
    )
    meta = _build_metadata_record(content, "test.md", "text/markdown", None, None)
    assert meta["source_url"] == "https://example.com/documents/42.pdf"


def test_url_column_also_sets_source_url():
    """url column also triggers source_url (same as file_url)."""
    content = "url:\nhttps://example.org/page\n\ntext:\nsome content\n\n"
    meta = _build_metadata_record(content, "test.md", "text/markdown", None, None)
    assert meta["source_url"] == "https://example.org/page"


def test_non_url_value_in_source_url_column_is_ignored():
    """A source_url column with a non-URL value (e.g. empty string) should not set source_url."""
    content = "source_url:\n\n\nsession_id:\n55\n\n"
    meta = _build_metadata_record(content, "test.md", "text/markdown", None, None)
    assert "source_url" not in meta


# -- CSV stream collector: URL columns must NOT pollute embedded content. --
# Hand-written CSVs in tmp_path; verifies that _collect_raw_streams_csv
# separates URL-typed columns out of the flattened markdown and returns
# them as a per-row extra_meta dict.

import csv as _csv  # noqa: E402 — keep this near the new test block
from pathlib import Path  # noqa: E402

from botnim.collect_sources import _collect_raw_streams_csv  # noqa: E402


def _write_csv(path: Path, rows: list[dict]) -> None:
    fieldnames = list(rows[0].keys())
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = _csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)


def test_csv_collector_excludes_source_url_from_content(tmp_path):
    """source_url column lands in extra_meta, NOT in the flattened markdown."""
    csv_path = tmp_path / "plenary.csv"
    _write_csv(csv_path, [
        {
            "session_id": "1234",
            "item_name": "חוק הביטוח הלאומי",
            "source_url": "https://fs.knesset.gov.il/25/plenum/25_st_99.doc",
        },
    ])
    out = _collect_raw_streams_csv(tmp_path, "plenary_schedule", "plenary.csv")
    assert len(out) == 1
    fname, content, ctype, extra_meta = out[0]
    assert "source_url" not in content
    assert "fs.knesset.gov.il" not in content
    assert "item_name:\nחוק הביטוח הלאומי" in content
    assert extra_meta == {"source_url": "https://fs.knesset.gov.il/25/plenum/25_st_99.doc"}


def test_csv_collector_empty_url_yields_no_extra_meta(tmp_path):
    """An empty source_url value (upcoming session) must not pollute extra_meta."""
    csv_path = tmp_path / "plenary.csv"
    _write_csv(csv_path, [
        {"session_id": "5555", "item_name": "x", "source_url": ""},
    ])
    out = _collect_raw_streams_csv(tmp_path, "plenary_schedule", "plenary.csv")
    fname, content, ctype, extra_meta = out[0]
    assert "source_url" not in content
    assert extra_meta == {}


def test_csv_collector_no_url_columns_unchanged(tmp_path):
    """CSV with no URL-typed columns: extra_meta empty, content includes all columns."""
    csv_path = tmp_path / "legal.csv"
    _write_csv(csv_path, [
        {"title": "חוק יסוד", "body": "abc"},
    ])
    out = _collect_raw_streams_csv(tmp_path, "legal_text", "legal.csv")
    fname, content, ctype, extra_meta = out[0]
    assert "title:\nחוק יסוד" in content
    assert "body:\nabc" in content
    assert extra_meta == {}


def test_lexicon_url_column_is_metadata_only(tmp_path):
    """lexicon_url column lands in extra_meta, NOT in the flattened content."""
    from botnim.collect_sources import _collect_raw_streams_csv

    csv_path = tmp_path / "lexicon.csv"
    csv_path.write_text(
        "מידע,lexicon_url,source_url\n"
        "שאילתות חבר הכנסת: סעיף 137 לתקנון.,"
        "https://main.knesset.gov.il/About/Lexicon/Pages/query.aspx,"
        "https://he.wikisource.org/wiki/תקנון_הכנסת#סעיף_137\n",
        encoding="utf-8",
    )
    out = _collect_raw_streams_csv(tmp_path, "common_takanon_knowledge", "lexicon.csv")
    assert len(out) == 1
    fname, content, ctype, extra_meta = out[0]
    # Neither URL should appear in the embedding content.
    assert "https://" not in content
    assert "lexicon_url" not in content
    # Both URLs should land in extra_meta.
    assert extra_meta["lexicon_url"] == (
        "https://main.knesset.gov.il/About/Lexicon/Pages/query.aspx"
    )
    assert extra_meta["source_url"] == (
        "https://he.wikisource.org/wiki/תקנון_הכנסת#סעיף_137"
    )


# -----------------------------------------------------------------------------
# Bookkeeping columns must not poison content_hash — 2026-05-20 fix
# -----------------------------------------------------------------------------

import hashlib as _hashlib  # noqa: E402


def test_csv_collector_drops_upstream_hash_from_content(tmp_path):
    """knesset_protocols' upstream_hash column is per-run provenance; it must
    NOT appear in the flattened content (would poison content_hash daily)."""
    csv_path = tmp_path / "kp.csv"
    _write_csv(csv_path, [
        {
            "upstream_hash": "a" * 64,
            "document_id": "537",
            "speaker_name": "ישראל ישראלי",
            "turn_text": "טקסט הדיון",
        },
    ])
    out = _collect_raw_streams_csv(tmp_path, "knesset_protocols", "kp.csv")
    fname, content, ctype, extra_meta = out[0]
    assert "upstream_hash" not in content
    assert "a" * 64 not in content
    # Real content survives.
    assert "speaker_name:\nישראל ישראלי" in content
    assert "turn_text:\nטקסט הדיון" in content
    # Bookkeeping is dropped entirely — not parked in metadata either.
    assert "upstream_hash" not in extra_meta


def test_csv_content_hash_stable_across_upstream_hash_change(tmp_path):
    """The core regression: two CSVs identical except for upstream_hash must
    produce byte-identical flattened content → identical content_hash. This
    is what lets the extraction cache actually hit for knesset_protocols."""
    row_a = {
        "upstream_hash": "1111111111111111111111111111111111111111111111111111111111111111",
        "document_id": "537",
        "turn_text": "אותו הטקסט בדיוק",
    }
    row_b = dict(row_a, upstream_hash="2222222222222222222222222222222222222222222222222222222222222222")

    csv_a = tmp_path / "a.csv"
    csv_b = tmp_path / "b.csv"
    _write_csv(csv_a, [row_a])
    _write_csv(csv_b, [row_b])

    content_a = _collect_raw_streams_csv(tmp_path, "knesset_protocols", "a.csv")[0][1]
    content_b = _collect_raw_streams_csv(tmp_path, "knesset_protocols", "b.csv")[0][1]

    assert content_a == content_b, "content must not vary with upstream_hash"
    h = lambda s: _hashlib.sha256(s.strip().encode("utf-8")).hexdigest()
    assert h(content_a) == h(content_b)


def test_csv_collector_drops_pdf_revision_columns(tmp_path):
    """PDF CSVs carry `revision` + `upstream_revision` — same poison, dropped."""
    csv_path = tmp_path / "pdf.csv"
    _write_csv(csv_path, [
        {
            "revision": "v7",
            "upstream_revision": "2026-05-20T03:00:00",
            "מספר_מסמך": "2024/123",
            "טקסט_מלא": "תוכן המסמך",
        },
    ])
    out = _collect_raw_streams_csv(tmp_path, "legal_advisor_opinions", "pdf.csv")
    fname, content, ctype, extra_meta = out[0]
    assert "revision" not in content
    assert "upstream_revision" not in content
    assert "v7" not in content
    assert "טקסט_מלא:\nתוכן המסמך" in content


# -----------------------------------------------------------------------------
# knesset_protocols' file_last_updated (= OData LastUpdatedDate) is per-document
# provenance that rotates whenever the upstream doc is touched. It MUST be
# dropped from content like upstream_hash — otherwise every turn re-hashes and
# the extraction cache never warms (observed in prod 2026-06-04: 334K cache rows
# for a ~134K-chunk corpus ≈ 2.5x bloat, 7-9K fresh rows every run, 0 hits).
# -----------------------------------------------------------------------------


def test_csv_collector_drops_file_last_updated_from_content(tmp_path):
    csv_path = tmp_path / "kp.csv"
    _write_csv(csv_path, [
        {
            "file_last_updated": "2026-06-04T11:10:54.293",
            "document_id": "537",
            "speaker_name": "ישראל ישראלי",
            "turn_text": "טקסט הדיון",
        },
    ])
    out = _collect_raw_streams_csv(tmp_path, "knesset_protocols", "kp.csv")
    fname, content, ctype, extra_meta = out[0]
    assert "file_last_updated" not in content
    assert "2026-06-04T11:10:54.293" not in content
    # Real content survives.
    assert "speaker_name:\nישראל ישראלי" in content
    assert "turn_text:\nטקסט הדיון" in content
    # Dropped entirely — not parked in metadata either.
    assert "file_last_updated" not in extra_meta


def test_csv_content_hash_stable_across_file_last_updated_change(tmp_path):
    """Core regression: two CSVs identical except for file_last_updated must
    produce byte-identical flattened content → identical content_hash, so the
    extraction cache can finally warm for knesset_protocols."""
    row_a = {
        "file_last_updated": "2026-06-04T11:10:54.293",
        "document_id": "537",
        "turn_text": "אותו הטקסט בדיוק",
    }
    row_b = dict(row_a, file_last_updated="2026-05-01T08:00:00.000")

    _write_csv(tmp_path / "a.csv", [row_a])
    _write_csv(tmp_path / "b.csv", [row_b])

    content_a = _collect_raw_streams_csv(tmp_path, "knesset_protocols", "a.csv")[0][1]
    content_b = _collect_raw_streams_csv(tmp_path, "knesset_protocols", "b.csv")[0][1]

    assert content_a == content_b, "content must not vary with file_last_updated"
    h = lambda s: _hashlib.sha256(s.strip().encode("utf-8")).hexdigest()
    assert h(content_a) == h(content_b)


# -----------------------------------------------------------------------------
# Glob support in _collect_raw_streams_split (Task 7 — israeli_laws context)
# -----------------------------------------------------------------------------

import json as _json  # noqa: E402

from botnim.collect_sources import _collect_raw_streams_split  # noqa: E402


def _write_law_json(path: Path, document_name: str, section_name: str, content: str):
    path.write_text(_json.dumps({
        "metadata": {"document_name": document_name},
        "structure": [{"depth": 1, "section_name": section_name,
                       "section_type": "סעיף", "content": content, "children": []}],
    }, ensure_ascii=False), encoding="utf-8")


def test_split_glob_reads_all_law_jsons(tmp_path: Path):
    d = tmp_path / "extraction" / "law_book"
    d.mkdir(parents=True)
    _write_law_json(d / "חוק_א_structure_content.json", "חוק א", "סעיף 1", "תוכן א")
    _write_law_json(d / "חוק_ב_structure_content.json", "חוק ב", "סעיף 1", "תוכן ב")

    out = _collect_raw_streams_split(tmp_path, "israeli_laws",
                                     "extraction/law_book/*_structure_content.json")
    names = sorted(fname for fname, _c, _t, _m in out)
    # document_name prefixes every chunk filename → no collision across laws.
    # sanitize_filename converts spaces to underscores in both document_name
    # and section_name components, so "חוק א" + "סעיף 1" → "חוק_א_סעיף_1.md".
    assert names == ["חוק_א_סעיף_1.md", "חוק_ב_סעיף_1.md"]
    bodies = " ".join(c for _f, c, _t, _m in out)
    assert "תוכן א" in bodies and "תוכן ב" in bodies
