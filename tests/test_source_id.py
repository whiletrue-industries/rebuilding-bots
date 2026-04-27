"""Unit tests for _source_id_for — pure derivation, no DB or network."""
from botnim.sync import _source_id_for


def test_wikitext_returns_decoded_last_path_segment():
    fetcher = {
        "kind": "wikitext",
        "input_url": "https://he.wikisource.org/wiki/%D7%97%D7%95%D7%A7_%D7%94%D7%9B%D7%A0%D7%A1%D7%AA",
    }
    assert _source_id_for(fetcher, None) == "חוק_הכנסת"


def test_wikitext_strips_url_fragment():
    fetcher = {
        "kind": "wikitext",
        "input_url": "https://he.wikisource.org/wiki/%D7%AA%D7%A7%D7%A0%D7%95%D7%9F_%D7%94%D7%9B%D7%A0%D7%A1%D7%AA#section_3",
    }
    assert _source_id_for(fetcher, None) == "תקנון_הכנסת"


def test_wikitext_handles_trailing_slash():
    fetcher = {
        "kind": "wikitext",
        "input_url": "https://he.wikisource.org/wiki/page/",
    }
    assert _source_id_for(fetcher, None) == "page"


def test_pdf_uses_basename_minus_extension():
    fetcher = {"kind": "pdf"}
    assert _source_id_for(fetcher, "extraction/knesset_committee_decisions.csv") == "knesset_committee_decisions"


def test_lexicon_returns_fixed_string():
    assert _source_id_for({"kind": "lexicon"}, None) == "lexicon"


def test_bk_csv_returns_fixed_string():
    assert _source_id_for({"kind": "bk_csv"}, None) == "bk_csv"


def test_no_fetcher_uses_source_basename():
    assert _source_id_for(None, "common-knowledge.md") == "common-knowledge"


def test_no_fetcher_no_source_returns_unknown():
    assert _source_id_for(None, None) == "unknown"
