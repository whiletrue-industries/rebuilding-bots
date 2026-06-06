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
# Hand-written CSVs placed at the correct ArtifactStore key path;
# verifies that _collect_raw_streams_csv separates URL-typed columns
# out of the flattened markdown and returns them as a per-row extra_meta dict.
#
# Key convention: writers produce key_for_extraction(bot, source) =
# "cache/<bot>/<source>". Tests use config_dir=tmp_path/"testbot" so
# config_dir.name == "testbot", giving store key "cache/testbot/<source>"
# under a LocalFsStore rooted at tmp_path.

import csv as _csv  # noqa: E402 — keep this near the new test block
import json  # noqa: E402
from pathlib import Path  # noqa: E402

import pytest  # noqa: E402

from botnim.collect_sources import _collect_raw_streams_csv  # noqa: E402
from botnim.storage import LocalFsStore  # noqa: E402
from botnim.storage.csv_writer import key_for_extraction  # noqa: E402

# Fixed bot name used across all CSV tests so config_dir.name is stable.
_BOT = "testbot"


@pytest.fixture(autouse=True)
def _store_at_tmp_path(tmp_path, monkeypatch):
    """Route the ArtifactStore singleton to a LocalFsStore rooted at tmp_path.

    All collector tests below pass ``tmp_path / _BOT`` as ``config_dir``
    (so ``config_dir.name == _BOT``) and write fixtures via ``_write_store_csv``
    / ``_store_put`` which place files at ``key_for_extraction(_BOT, source)``
    within ``tmp_path``. The autouse fixture points the module-level
    ``get_artifact_store`` at that same root so the reader resolves them.
    """
    import botnim.collect_sources as cs
    monkeypatch.setattr(cs, "get_artifact_store", lambda: LocalFsStore(str(tmp_path)))


def _config_dir(tmp_path: Path) -> Path:
    """Return (and create) the per-test config_dir whose .name == _BOT."""
    d = tmp_path / _BOT
    d.mkdir(exist_ok=True)
    return d


def _write_store_csv(tmp_path: Path, source: str, rows: list[dict]) -> None:
    """Write CSV rows to the ArtifactStore key path under tmp_path."""
    key = key_for_extraction(_BOT, source)
    dest = tmp_path / key
    dest.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(rows[0].keys())
    with open(dest, "w", encoding="utf-8", newline="") as f:
        w = _csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)


def _store_put(tmp_path: Path, source: str, text: str) -> None:
    """Write raw text content to the ArtifactStore key path under tmp_path."""
    key = key_for_extraction(_BOT, source)
    dest = tmp_path / key
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(text, encoding="utf-8")


def test_csv_collector_excludes_source_url_from_content(tmp_path):
    """source_url column lands in extra_meta, NOT in the flattened markdown."""
    _write_store_csv(tmp_path, "plenary.csv", [
        {
            "session_id": "1234",
            "item_name": "חוק הביטוח הלאומי",
            "source_url": "https://fs.knesset.gov.il/25/plenum/25_st_99.doc",
        },
    ])
    out = _collect_raw_streams_csv(_config_dir(tmp_path), "plenary_schedule", "plenary.csv")
    assert len(out) == 1
    fname, content, ctype, extra_meta = out[0]
    assert "source_url" not in content
    assert "fs.knesset.gov.il" not in content
    assert "item_name:\nחוק הביטוח הלאומי" in content
    assert extra_meta == {"source_url": "https://fs.knesset.gov.il/25/plenum/25_st_99.doc"}


def test_csv_collector_empty_url_yields_no_extra_meta(tmp_path):
    """An empty source_url value (upcoming session) must not pollute extra_meta."""
    _write_store_csv(tmp_path, "plenary.csv", [
        {"session_id": "5555", "item_name": "x", "source_url": ""},
    ])
    out = _collect_raw_streams_csv(_config_dir(tmp_path), "plenary_schedule", "plenary.csv")
    fname, content, ctype, extra_meta = out[0]
    assert "source_url" not in content
    assert extra_meta == {}


def test_csv_collector_no_url_columns_unchanged(tmp_path):
    """CSV with no URL-typed columns: extra_meta empty, content includes all columns."""
    _write_store_csv(tmp_path, "legal.csv", [
        {"title": "חוק יסוד", "body": "abc"},
    ])
    out = _collect_raw_streams_csv(_config_dir(tmp_path), "legal_text", "legal.csv")
    fname, content, ctype, extra_meta = out[0]
    assert "title:\nחוק יסוד" in content
    assert "body:\nabc" in content
    assert extra_meta == {}


def test_lexicon_url_column_is_metadata_only(tmp_path):
    """lexicon_url column lands in extra_meta, NOT in the flattened content."""
    from botnim.collect_sources import _collect_raw_streams_csv

    _store_put(
        tmp_path,
        "lexicon.csv",
        "מידע,lexicon_url,source_url\n"
        "שאילתות חבר הכנסת: סעיף 137 לתקנון.,"
        "https://main.knesset.gov.il/About/Lexicon/Pages/query.aspx,"
        "https://he.wikisource.org/wiki/תקנון_הכנסת#סעיף_137\n",
    )
    out = _collect_raw_streams_csv(_config_dir(tmp_path), "common_takanon_knowledge", "lexicon.csv")
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
    _write_store_csv(tmp_path, "kp.csv", [
        {
            "upstream_hash": "a" * 64,
            "document_id": "537",
            "speaker_name": "ישראל ישראלי",
            "turn_text": "טקסט הדיון",
        },
    ])
    out = _collect_raw_streams_csv(_config_dir(tmp_path), "knesset_protocols", "kp.csv")
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

    _write_store_csv(tmp_path, "a.csv", [row_a])
    _write_store_csv(tmp_path, "b.csv", [row_b])

    content_a = _collect_raw_streams_csv(_config_dir(tmp_path), "knesset_protocols", "a.csv")[0][1]
    content_b = _collect_raw_streams_csv(_config_dir(tmp_path), "knesset_protocols", "b.csv")[0][1]

    assert content_a == content_b, "content must not vary with upstream_hash"
    h = lambda s: _hashlib.sha256(s.strip().encode("utf-8")).hexdigest()
    assert h(content_a) == h(content_b)


def test_csv_collector_drops_pdf_revision_columns(tmp_path):
    """PDF CSVs carry `revision` + `upstream_revision` — same poison, dropped."""
    _write_store_csv(tmp_path, "pdf.csv", [
        {
            "revision": "v7",
            "upstream_revision": "2026-05-20T03:00:00",
            "מספר_מסמך": "2024/123",
            "טקסט_מלא": "תוכן המסמך",
        },
    ])
    out = _collect_raw_streams_csv(_config_dir(tmp_path), "legal_advisor_opinions", "pdf.csv")
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
    _write_store_csv(tmp_path, "kp.csv", [
        {
            "file_last_updated": "2026-06-04T11:10:54.293",
            "document_id": "537",
            "speaker_name": "ישראל ישראלי",
            "turn_text": "טקסט הדיון",
        },
    ])
    out = _collect_raw_streams_csv(_config_dir(tmp_path), "knesset_protocols", "kp.csv")
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

    _write_store_csv(tmp_path, "a.csv", [row_a])
    _write_store_csv(tmp_path, "b.csv", [row_b])

    content_a = _collect_raw_streams_csv(_config_dir(tmp_path), "knesset_protocols", "a.csv")[0][1]
    content_b = _collect_raw_streams_csv(_config_dir(tmp_path), "knesset_protocols", "b.csv")[0][1]

    assert content_a == content_b, "content must not vary with file_last_updated"
    h = lambda s: _hashlib.sha256(s.strip().encode("utf-8")).hexdigest()
    assert h(content_a) == h(content_b)


# -----------------------------------------------------------------------------
# CLUSTER D — read side resolves through the ArtifactStore, not config_dir.
# -----------------------------------------------------------------------------

from botnim.collect_sources import _collect_raw_streams_files  # noqa: E402
from botnim.collect_sources import _collect_raw_streams_split  # noqa: E402


def test_files_collector_reads_via_store(tmp_path, monkeypatch):
    """`files` source type globs via store.list(prefix) and opens via store.open_stream.

    Writer key = key_for_extraction(bot, source) = "cache/<bot>/<source>".
    Verify the reader resolves the exact same key.
    """
    # Write files at the correct store key path (cache/testbot/extraction/legal_text/...)
    source_a = "extraction/legal_text/a.md"
    source_b = "extraction/legal_text/b.md"
    store = LocalFsStore(str(tmp_path))
    store.put_atomic(key_for_extraction(_BOT, source_a), b"ALPHA")
    store.put_atomic(key_for_extraction(_BOT, source_b), b"BETA")

    import botnim.collect_sources as cs
    monkeypatch.setattr(cs, "get_artifact_store", lambda: store)

    out = _collect_raw_streams_files(_config_dir(tmp_path), "extraction/legal_text/*.md")
    names = sorted(fname for fname, _, _, _ in out)
    assert names == ["a.md", "b.md"]
    bodies = {}
    for fname, stream, ctype, extra in out:
        assert ctype == "text/markdown"
        assert extra == {}
        bodies[fname] = stream.read().decode("utf-8") if hasattr(stream, "read") else stream
    assert bodies == {"a.md": "ALPHA", "b.md": "BETA"}


def test_split_collector_reads_dashsep_via_store(tmp_path, monkeypatch):
    """`split` source of a plain .md splits on '\\n---\\n' and reads via store.get_bytes."""
    source = "extraction/doc.md"
    store = LocalFsStore(str(tmp_path))
    store.put_atomic(key_for_extraction(_BOT, source), b"PART_ONE\n---\nPART_TWO")

    import botnim.collect_sources as cs
    monkeypatch.setattr(cs, "get_artifact_store", lambda: store)

    out = _collect_raw_streams_split(_config_dir(tmp_path), "legal_text", source)
    assert [c for _, c, _, _ in out] == ["PART_ONE", "PART_TWO"]
    assert [fname for fname, _, _, _ in out] == ["legal_text_0.md", "legal_text_1.md"]


def test_split_collector_reads_json_via_store(tmp_path, monkeypatch):
    """`split` source of a .json builds the markdown dict from store.get_bytes."""
    source = "extraction/struct.json"
    payload = {
        "metadata": {"document_name": "תקנון"},
        "structure": [{"type": "section", "title": "ס", "level": 1, "content": "גוף"}],
    }
    store = LocalFsStore(str(tmp_path))
    store.put_atomic(
        key_for_extraction(_BOT, source),
        json.dumps(payload, ensure_ascii=False).encode("utf-8"),
    )

    import botnim.collect_sources as cs
    monkeypatch.setattr(cs, "get_artifact_store", lambda: store)

    out = _collect_raw_streams_split(_config_dir(tmp_path), "legal_text", source)
    # generate_markdown_dict produces at least one (filename, content) entry.
    assert len(out) >= 1
    assert all(ctype == "text/markdown" and extra == {} for _, _, ctype, extra in out)


def test_csv_collector_reads_via_store_not_config_dir(tmp_path, monkeypatch):
    """The CSV reader must resolve via the store, ignoring config_dir's filesystem
    path entirely. We put the file in the store but NOT at config_dir on disk."""
    source = "x.csv"
    # Store rooted at tmp_path/store_root — completely different from config_dir.
    store_root = tmp_path / "store_root"
    store = LocalFsStore(str(store_root))
    store.put_atomic(key_for_extraction(_BOT, source), b"title,body\nT,B\n")

    import botnim.collect_sources as cs
    monkeypatch.setattr(cs, "get_artifact_store", lambda: store)

    # config_dir is tmp_path/testbot — does NOT have x.csv on disk.
    out = _collect_raw_streams_csv(_config_dir(tmp_path), "legal_text", source)
    assert len(out) == 1
    fname, content, ctype, extra_meta = out[0]
    assert "title:\nT" in content
    assert "body:\nB" in content
    assert extra_meta == {}


# WRITE→READ round-trip: writer stores at key_for_extraction; reader reads it back.
def test_csv_writer_reader_round_trip(tmp_path, monkeypatch):
    """End-to-end key agreement: write an artifact via the store at
    key_for_extraction, then have _collect_raw_streams_csv read it back and
    assert we get the same rows — proving writer/reader key convention matches."""
    from botnim.storage.csv_writer import write_csv_artifact

    source = "extraction/x.csv"
    store = LocalFsStore(str(tmp_path))
    write_key = key_for_extraction(_BOT, source)
    rows = [{"col_a": "hello", "col_b": "world"}]
    write_csv_artifact(store, write_key, rows, fieldnames=["col_a", "col_b"])

    import botnim.collect_sources as cs
    monkeypatch.setattr(cs, "get_artifact_store", lambda: store)

    out = _collect_raw_streams_csv(_config_dir(tmp_path), "myctx", source)
    assert len(out) == 1
    _, content, ctype, extra_meta = out[0]
    assert "col_a:\nhello" in content
    assert "col_b:\nworld" in content
    assert ctype == "text/markdown"
    assert extra_meta == {}


# WRITE→READ round-trip for the files/split readers.
def test_files_writer_reader_round_trip(tmp_path, monkeypatch):
    """End-to-end: write markdown bytes via store.put_atomic at key_for_extraction,
    then read back through _collect_raw_streams_files — confirms key agreement."""
    source_pattern = "extraction/docs/*.md"
    source_file = "extraction/docs/hello.md"
    store = LocalFsStore(str(tmp_path))
    store.put_atomic(key_for_extraction(_BOT, source_file), b"HELLO WORLD")

    import botnim.collect_sources as cs
    monkeypatch.setattr(cs, "get_artifact_store", lambda: store)

    out = _collect_raw_streams_files(_config_dir(tmp_path), source_pattern)
    assert len(out) == 1
    fname, stream, ctype, extra = out[0]
    assert fname == "hello.md"
    assert stream.read() == b"HELLO WORLD"
    assert ctype == "text/markdown"


# -----------------------------------------------------------------------------
# CLUSTER D — moto S3 read coverage.
# -----------------------------------------------------------------------------

import boto3  # noqa: E402
from moto import mock_aws  # noqa: E402

from botnim.storage import S3Store  # noqa: E402


def test_files_and_csv_collectors_read_from_s3(monkeypatch):
    """`files` glob and `csv` reader both resolve through S3Store under moto.

    Objects are written at key_for_extraction(_BOT, source) to confirm the
    reader and writer agree on the key convention against a real S3 backend.
    """
    with mock_aws():
        monkeypatch.setenv("AWS_DEFAULT_REGION", "il-central-1")
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
        s3 = boto3.client("s3", region_name="il-central-1")
        s3.create_bucket(
            Bucket="botnim-artifacts-test",
            CreateBucketConfiguration={"LocationConstraint": "il-central-1"},
        )
        files_source = "extraction/legal_text/a.md"
        csv_source = "extraction/data/rows.csv"
        s3.put_object(
            Bucket="botnim-artifacts-test",
            Key=key_for_extraction(_BOT, files_source),
            Body=b"ALPHA",
        )
        s3.put_object(
            Bucket="botnim-artifacts-test",
            Key=key_for_extraction(_BOT, csv_source),
            Body=b"title,body\nT,B\n",
        )

        store = S3Store("botnim-artifacts-test")
        import botnim.collect_sources as cs
        monkeypatch.setattr(cs, "get_artifact_store", lambda: store)

        config_dir = Path("/unused") / _BOT

        files_out = _collect_raw_streams_files(config_dir, "extraction/legal_text/*.md")
        assert len(files_out) == 1
        assert [fname for fname, _, _, _ in files_out] == ["a.md"]
        assert files_out[0][1].read().decode("utf-8") == "ALPHA"

        csv_out = _collect_raw_streams_csv(config_dir, "legal_text", csv_source)
        assert len(csv_out) == 1
        _, content, ctype, extra_meta = csv_out[0]
        assert "title:\nT" in content
        assert "body:\nB" in content
        assert extra_meta == {}


# -----------------------------------------------------------------------------
# Dispatcher regression test: _raw_streams_for_context delegates to
# store-backed reader through the unchanged dispatcher.
# -----------------------------------------------------------------------------

from botnim.collect_sources import _raw_streams_for_context  # noqa: E402


def test_dispatcher_csv_through_store(tmp_path, monkeypatch):
    """_raw_streams_for_context('csv') delegates to the store-backed reader and
    appends source_id to each tuple (5-tuple shape)."""
    source = "extraction/d.csv"
    _write_store_csv(tmp_path, source, [{"title": "T", "body": "B"}])

    import botnim.collect_sources as cs
    monkeypatch.setattr(cs, "get_artifact_store", lambda: LocalFsStore(str(tmp_path)))

    context_ = {"name": "legal_text", "type": "csv", "source": source}
    out = _raw_streams_for_context(_config_dir(tmp_path), "legal_text", context_)
    assert len(out) == 1
    fname, content, ctype, source_id, extra_meta = out[0]
    assert "title:\nT" in content
    assert ctype == "text/markdown"
    assert isinstance(source_id, str) and source_id  # derived from _source_id_for
