# tests/test_law_book_process.py
from pathlib import Path
from unittest.mock import patch, MagicMock
import yaml
from botnim.document_parser.wikisource_law_book import process as P
from botnim.document_parser.wikisource_law_book.manifest import LawBookEntry, read_manifest


def _cfg(tmp_path):
    (tmp_path / "config.yaml").write_text(
        yaml.safe_dump({"context": [{"slug": "legal_text", "sources": []}]}, allow_unicode=True),
        encoding="utf-8")


def test_process_runs_processor_per_item_and_records_status(tmp_path: Path):
    _cfg(tmp_path)
    discovered = [
        LawBookEntry("חוק האזנת סתר", "https://he.wikisource.org/wiki/חוק_האזנת_סתר", "law"),
        LawBookEntry("חוק אוויר נקי", "https://he.wikisource.org/wiki/חוק_אוויר_נקי", "law"),
    ]
    made = MagicMock()
    made.run.return_value = True
    with patch.object(P, "discover_law_pages", return_value=discovered), \
         patch.object(P, "WikitextProcessorConfig") as MockCfg, \
         patch.object(P, "WikitextProcessor", return_value=made) as MockProc:
        P.process_law_book_source("staging", tmp_path, include_regulations=False,
                                  min_expected_laws=1, rate_limit_seconds=0)
    assert MockProc.call_count == 2
    out = {e.title: e.status for e in read_manifest(tmp_path / "extraction" / "law_book" / "manifest.csv")}
    assert out == {"חוק האזנת סתר": "ok", "חוק אוויר נקי": "ok"}


def test_process_isolates_per_item_failure(tmp_path: Path):
    _cfg(tmp_path)
    discovered = [
        LawBookEntry("חוק טוב", "https://he.wikisource.org/wiki/חוק_טוב", "law"),
        LawBookEntry("חוק רע", "https://he.wikisource.org/wiki/חוק_רע", "law"),
    ]

    def cfg_side_effect(*a, **k):
        if "רע" in k.get("input_url", ""):
            raise RuntimeError("network boom")
        return MagicMock()

    good = MagicMock(); good.run.return_value = True
    with patch.object(P, "discover_law_pages", return_value=discovered), \
         patch.object(P, "WikitextProcessorConfig", side_effect=cfg_side_effect), \
         patch.object(P, "WikitextProcessor", return_value=good):
        P.process_law_book_source("staging", tmp_path, include_regulations=False,
                                  min_expected_laws=1, rate_limit_seconds=0)
    out = {e.title: e.status for e in read_manifest(tmp_path / "extraction" / "law_book" / "manifest.csv")}
    assert out == {"חוק טוב": "ok", "חוק רע": "failed"}
