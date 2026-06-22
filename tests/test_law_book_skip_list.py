from pathlib import Path
import yaml
from botnim.document_parser.wikisource_law_book.skip_list import (
    title_from_url, legal_text_skip_titles,
)


def test_title_from_url_decodes_and_unscores():
    url = "https://he.wikisource.org/wiki/%D7%97%D7%95%D7%A7_%D7%94%D7%9B%D7%A0%D7%A1%D7%AA"
    assert title_from_url(url) == "חוק הכנסת"


def test_skip_titles_reads_legal_text(tmp_path: Path):
    cfg = {
        "context": [
            {"slug": "legal_text", "sources": [
                {"type": "split", "source": "x.json",
                 "fetcher": {"kind": "wikitext",
                             "input_url": "https://he.wikisource.org/wiki/%D7%97%D7%95%D7%A7_%D7%94%D7%9B%D7%A0%D7%A1%D7%AA"}},
            ]},
            {"slug": "other", "sources": []},
        ]
    }
    (tmp_path / "config.yaml").write_text(yaml.safe_dump(cfg, allow_unicode=True), encoding="utf-8")
    assert legal_text_skip_titles(tmp_path) == {"חוק הכנסת"}
