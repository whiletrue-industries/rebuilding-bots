# tests/test_law_book_enumerate.py
from pathlib import Path
from unittest.mock import patch
import yaml
import pytest
from botnim.document_parser.wikisource_law_book import enumerate_laws as E
from botnim.document_parser.wikisource_law_book.manifest import LawBookEntry


def _write_cfg(tmp_path):
    cfg = {"context": [{"slug": "legal_text", "sources": [
        {"type": "split", "source": "x.json",
         "fetcher": {"kind": "wikitext",
                     "input_url": "https://he.wikisource.org/wiki/חוק_הכנסת"}}]}]}
    (tmp_path / "config.yaml").write_text(yaml.safe_dump(cfg, allow_unicode=True), encoding="utf-8")


def test_discover_classifies_skips_and_filters(tmp_path: Path):
    _write_cfg(tmp_path)
    titles = ["חוק האזנת סתר", "חוק הכנסת", "תקנות הגנת הפרטיות", "ויקיטקסט:אודות"]
    with patch.object(E, "fetch_index_titles", return_value=titles):
        # laws only: drops the regulation, the skip-list law (חוק הכנסת), the noise page
        out = E.discover_law_pages(tmp_path, include_regulations=False, min_expected_laws=1)
    assert [e.title for e in out] == ["חוק האזנת סתר"]
    assert out[0].kind == "law"
    assert out[0].url == "https://he.wikisource.org/wiki/חוק_האזנת_סתר"
    assert out[0].status == "pending"


def test_discover_includes_regulations_when_flagged(tmp_path: Path):
    _write_cfg(tmp_path)
    titles = ["חוק האזנת סתר", "תקנות הגנת הפרטיות"]
    with patch.object(E, "fetch_index_titles", return_value=titles):
        out = E.discover_law_pages(tmp_path, include_regulations=True, min_expected_laws=1)
    assert {e.kind for e in out} == {"law", "regulation"}


def test_discover_floor_guard(tmp_path: Path):
    _write_cfg(tmp_path)
    with patch.object(E, "fetch_index_titles", return_value=["חוק האזנת סתר"]):
        with pytest.raises(E.CoverageShrinkError):
            E.discover_law_pages(tmp_path, include_regulations=False, min_expected_laws=200)


def test_discover_shrink_guard_vs_prior(tmp_path: Path):
    _write_cfg(tmp_path)
    prior = [LawBookEntry(f"חוק מספר {i}", f"u{i}", "law") for i in range(100)]
    with patch.object(E, "fetch_index_titles", return_value=["חוק האזנת סתר"]):
        with pytest.raises(E.CoverageShrinkError):
            E.discover_law_pages(tmp_path, include_regulations=False, min_expected_laws=1, prior=prior)


def test_discover_skip_list_disabled_includes_skiplisted_laws(tmp_path: Path):
    # _write_cfg skip-lists חוק הכנסת (it's a legal_text source). With
    # apply_skip_list=False the enumerator must NOT drop it — this is the
    # consolidation lever that lets israeli_laws ingest the legal_text laws
    # (incl. תקנון הכנסת) while legal_text still exists for the parity gate.
    _write_cfg(tmp_path)
    titles = ["חוק האזנת סתר", "חוק הכנסת"]
    with patch.object(E, "fetch_index_titles", return_value=titles):
        out = E.discover_law_pages(tmp_path, include_regulations=False,
                                   min_expected_laws=1, apply_skip_list=False)
    out_titles = [e.title for e in out]
    assert "חוק הכנסת" in out_titles      # normally skip-listed, now included
    assert "חוק האזנת סתר" in out_titles
