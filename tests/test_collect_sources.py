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
