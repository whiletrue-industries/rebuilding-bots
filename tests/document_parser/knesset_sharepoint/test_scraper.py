"""Unit tests for botnim.document_parser.knesset_sharepoint.scraper.

Live integration smoke (against the real Reblaze-walled SharePoint) is
gated behind the ``KNESSET_SCRAPER_LIVE=1`` env var so it doesn't run
in CI by default. The unit tests below exercise the pure logic
(filename derivation, atomic CSV write, empty-result safety guard,
ethics sub-page extractor) without needing Playwright.
"""
from __future__ import annotations

import csv
import io
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from botnim.document_parser.knesset_sharepoint.scraper import (
    EmptyUpstreamIndex,
    PdfRow,
    ScrapeConfig,
    _absolute,
    _atomic_write_csv,
    _default_row_extractor,
    _ensure_at_least_one_row,
    _ethics_row_extractor,
    _ethics_sub_index_extractor,
    _filename_for,
    ethics_committee_decisions_config,
    legal_advisor_letters_config,
    legal_advisor_opinions_config,
    scrape_pdf_index,
)
from botnim.storage.local_fs import LocalFsStore

_KEY = "cache/unified/extraction/legal_advisor_opinions.csv"
_KEY_LETTERS = "cache/unified/extraction/legal_advisor_letters.csv"


# ---------- Pure helpers ----------

def test_filename_for_is_md5_truncated():
    fn = _filename_for("https://fs.knesset.gov.il/19/foo.pdf")
    assert fn.endswith(".pdf")
    assert len(fn) == 20  # 16 hex chars + ".pdf"


def test_absolute_passes_through_http_urls():
    assert _absolute("https://x/y") == "https://x/y"
    assert _absolute("http://x/y") == "http://x/y"


def test_absolute_prefixes_relative_paths():
    assert _absolute("/foo/bar.pdf") == "https://main.knesset.gov.il/foo/bar.pdf"


# ---------- CSV write via store ----------

def test_atomic_write_csv_round_trips_rows(tmp_path: Path):
    store = LocalFsStore(tmp_path)
    key = _KEY
    rows = [
        PdfRow(url="https://fs.knesset.gov.il/a.pdf", title="Document A",
               filename=_filename_for("https://fs.knesset.gov.il/a.pdf")),
        PdfRow(url="https://fs.knesset.gov.il/b.pdf", title="Document B",
               filename=_filename_for("https://fs.knesset.gov.il/b.pdf"),
               date="2024-11-03", knesset_num=25),
    ]
    _atomic_write_csv(store, key, rows)
    assert store.exists(key)
    text = store.get_bytes(key).decode("utf-8")
    loaded = list(csv.DictReader(io.StringIO(text)))
    assert len(loaded) == 2
    assert loaded[0]["url"] == "https://fs.knesset.gov.il/a.pdf"
    assert loaded[0]["date"] == ""
    assert loaded[0]["knesset_num"] == ""
    assert loaded[1]["date"] == "2024-11-03"
    assert loaded[1]["knesset_num"] == "25"


def test_atomic_write_creates_nested_key(tmp_path: Path):
    store = LocalFsStore(tmp_path)
    key = "cache/unified/extraction/deep/nested/missing/index.csv"
    _atomic_write_csv(store, key, [PdfRow(url="https://x", title="t", filename="f.pdf")])
    assert store.exists(key)


# ---------- Empty-result safety guard ----------

def test_empty_rows_with_no_existing_key_is_allowed(tmp_path: Path):
    store = LocalFsStore(tmp_path)
    # No key exists yet — should not raise.
    _ensure_at_least_one_row([], page_url="https://fake", store=store, key=_KEY)


def test_empty_rows_with_populated_existing_key_raises(tmp_path: Path):
    store = LocalFsStore(tmp_path)
    # Pre-seed the store with one row.
    _atomic_write_csv(store, _KEY, [PdfRow(url="https://x.pdf", title="t", filename="f.pdf")])
    with pytest.raises(EmptyUpstreamIndex) as exc:
        _ensure_at_least_one_row([], page_url="https://fake", store=store, key=_KEY)
    assert "scraped 0 rows" in str(exc.value)
    assert "1 existing rows" in str(exc.value)


def test_empty_rows_with_header_only_key_is_allowed(tmp_path: Path):
    """A key that exists but contains only the header (0 data rows) should not
    trigger the guard — that is an empty-upstream first-run case."""
    store = LocalFsStore(tmp_path)
    header_only = b"url,title,filename,date,knesset_num\r\n"
    store.put_atomic(_KEY, header_only)
    # Should not raise because existing data rows == 0.
    _ensure_at_least_one_row([], page_url="https://fake", store=store, key=_KEY)


# ---------- Default row extractor ----------

def test_default_row_extractor_skips_non_pdf_anchors():
    extractor = _default_row_extractor("a")
    page = MagicMock()
    a1 = MagicMock()
    a1.get_attribute.return_value = "/foo/bar.pdf"
    a1.inner_text.return_value = "PDF Title"
    a2 = MagicMock()
    a2.get_attribute.return_value = "/foo/bar.html"
    a2.inner_text.return_value = "HTML Page"
    page.query_selector_all.return_value = [a1, a2]
    rows = list(extractor(page))
    assert len(rows) == 1
    assert rows[0].url == "https://main.knesset.gov.il/foo/bar.pdf"
    assert rows[0].title == "PDF Title"


def test_default_row_extractor_strips_title_whitespace():
    extractor = _default_row_extractor("a")
    page = MagicMock()
    a = MagicMock()
    a.get_attribute.return_value = "https://fs.knesset.gov.il/x.pdf"
    a.inner_text.return_value = "  spaced title  \n"
    page.query_selector_all.return_value = [a]
    rows = list(extractor(page))
    assert rows[0].title == "spaced title"


# ---------- Ethics-specific extractors ----------

def test_ethics_sub_index_extractor_finds_per_knesset_pages():
    html = (
        '<html><a href="/Activity/committees/Ethics/pages/CommitteeDecisions24.aspx">24</a>'
        '<a href="/Activity/committees/Ethics/pages/CommitteeDecisions22.aspx">22</a>'
        '<a href="/Activity/committees/Ethics/pages/CommitteeDecisions22.aspx">dup</a>'
        '</html>'
    )
    urls = _ethics_sub_index_extractor(html)
    assert "https://main.knesset.gov.il/Activity/committees/Ethics/pages/CommitteeDecisions24.aspx" in urls
    assert "https://main.knesset.gov.il/Activity/committees/Ethics/pages/CommitteeDecisions22.aspx" in urls
    # Duplicates are de-duped.
    assert sum(1 for u in urls if "CommitteeDecisions22" in u) == 1
    # Current-Knesset page is always included.
    assert any(u.endswith("CommitteeDecisions25.aspx") for u in urls)


def test_ethics_sub_index_extractor_falls_back_to_current_when_empty():
    urls = _ethics_sub_index_extractor("<html>nothing relevant</html>")
    assert urls == [
        "https://main.knesset.gov.il/Activity/committees/Ethics/pages/CommitteeDecisions25.aspx"
    ]


def test_ethics_row_extractor_pulls_date_and_knesset_num():
    page = MagicMock()
    page.url = "https://main.knesset.gov.il/Activity/committees/Ethics/pages/CommitteeDecisions24.aspx"
    container = MagicMock()
    anchor = MagicMock()
    anchor.get_attribute.return_value = "/eth/decision_001.pdf"
    anchor.inner_text.return_value = "Decision 001"
    date_node = MagicMock()
    date_node.inner_text.return_value = "12/05/2023"
    container.query_selector_all.side_effect = lambda sel: (
        [anchor] if sel == "a"
        else [date_node] if "ComEthics" in sel
        else []
    )
    page.query_selector_all.return_value = [container]
    rows = list(_ethics_row_extractor(page))
    assert len(rows) == 1
    assert rows[0].url == "https://main.knesset.gov.il/eth/decision_001.pdf"
    assert rows[0].date == "12/05/2023"
    assert rows[0].knesset_num == 24


def test_ethics_row_extractor_skips_rows_with_multi_anchors():
    page = MagicMock()
    page.url = "https://main.knesset.gov.il/Activity/committees/Ethics/pages/CommitteeDecisions25.aspx"
    container = MagicMock()
    a1, a2 = MagicMock(), MagicMock()
    a1.get_attribute.return_value = "/x.pdf"
    a2.get_attribute.return_value = "/y.pdf"
    date_node = MagicMock()
    date_node.inner_text.return_value = "01/01/2024"
    container.query_selector_all.side_effect = lambda sel: (
        [a1, a2] if sel == "a"
        else [date_node] if "ComEthics" in sel
        else []
    )
    page.query_selector_all.return_value = [container]
    rows = list(_ethics_row_extractor(page))
    assert rows == []


# ---------- Preset configs ----------

def test_legal_advisor_opinions_config_uses_correct_selector(tmp_path: Path):
    store = LocalFsStore(tmp_path)
    cfg = legal_advisor_opinions_config(store=store, key=_KEY)
    assert cfg.anchor_selector == "a.LDDocLink"
    assert cfg.page_url.endswith("/ldguidelines.aspx")
    assert cfg.sub_index_extractor is None  # single page, no sub-pages
    assert cfg.store is store
    assert cfg.key == _KEY


def test_legal_advisor_letters_config_uses_correct_selector(tmp_path: Path):
    store = LocalFsStore(tmp_path)
    cfg = legal_advisor_letters_config(store=store, key=_KEY_LETTERS)
    assert cfg.anchor_selector == "a.LDDocLink"
    assert cfg.page_url.endswith("/ldguidelines2.aspx")
    assert cfg.sub_index_extractor is None
    assert cfg.store is store
    assert cfg.key == _KEY_LETTERS


def test_ethics_config_wires_sub_index_and_custom_extractor(tmp_path: Path):
    store = LocalFsStore(tmp_path)
    cfg = ethics_committee_decisions_config(Path("/tmp/x.csv"))
    assert cfg.sub_index_extractor is _ethics_sub_index_extractor
    assert cfg.row_extractor is _ethics_row_extractor
    assert "CommitteeDecisionsPast" in cfg.page_url


# ---------- High-level scrape_pdf_index with mocked Playwright ----------

@pytest.fixture
def fake_playwright():
    """Inject mock ``playwright.sync_api`` + ``playwright_stealth`` modules
    so ``scrape_pdf_index`` can run without a real browser."""
    import sys
    import types

    page = MagicMock()
    page.url = "https://main.knesset.gov.il/test"
    ctx = MagicMock()
    ctx.cookies.return_value = [{"name": "rbz", "value": "x"}]
    ctx.new_page.return_value = page
    browser = MagicMock()
    browser.new_context.return_value = ctx
    chromium = MagicMock()
    chromium.launch.return_value = browser

    pw_obj = MagicMock()
    pw_obj.chromium = chromium

    pw_cm = MagicMock()
    pw_cm.__enter__ = MagicMock(return_value=pw_obj)
    pw_cm.__exit__ = MagicMock(return_value=False)

    sp_callable = MagicMock(return_value=pw_cm)
    pw_module = types.ModuleType("playwright")
    pw_sync_api = types.ModuleType("playwright.sync_api")
    pw_sync_api.sync_playwright = sp_callable

    stealth_instance = MagicMock()
    stealth_instance.use_sync.return_value = pw_cm
    stealth_class = MagicMock(return_value=stealth_instance)
    stealth_module = types.ModuleType("playwright_stealth")
    stealth_module.Stealth = stealth_class

    saved = {k: sys.modules.get(k) for k in ("playwright", "playwright.sync_api", "playwright_stealth")}
    sys.modules["playwright"] = pw_module
    sys.modules["playwright.sync_api"] = pw_sync_api
    sys.modules["playwright_stealth"] = stealth_module
    try:
        yield {"page": page, "browser": browser, "ctx": ctx}
    finally:
        for k, v in saved.items():
            if v is None:
                sys.modules.pop(k, None)
            else:
                sys.modules[k] = v


def test_scrape_pdf_index_writes_rows(tmp_path: Path, fake_playwright):
    store = LocalFsStore(tmp_path)
    page = fake_playwright["page"]

    a1 = MagicMock()
    a1.get_attribute.return_value = "/leg/op_001.pdf"
    a1.inner_text.return_value = "Opinion 1"
    a2 = MagicMock()
    a2.get_attribute.return_value = "/leg/op_002.pdf"
    a2.inner_text.return_value = "Opinion 2"
    page.query_selector_all.return_value = [a1, a2]
    page.content.return_value = "<html/>"

    cfg = ScrapeConfig(
        page_url="https://main.knesset.gov.il/about/departments/pages/leg/ldguidelines.aspx",
        anchor_selector="a.LDDocLink",
        store=store,
        key=_KEY,
    )
    rows = scrape_pdf_index(cfg)
    assert len(rows) == 2
    assert store.exists(_KEY)
    text = store.get_bytes(_KEY).decode("utf-8")
    loaded = list(csv.DictReader(io.StringIO(text)))
    assert [r["url"] for r in loaded] == [
        "https://main.knesset.gov.il/leg/op_001.pdf",
        "https://main.knesset.gov.il/leg/op_002.pdf",
    ]


def test_scrape_pdf_index_dedupes_across_subpages(tmp_path: Path, fake_playwright):
    """Ethics-style: two sub-pages may both link to the same PDF; only
    one row should land in the store."""
    store = LocalFsStore(tmp_path)
    page = fake_playwright["page"]

    a = MagicMock()
    a.get_attribute.return_value = "/eth/x.pdf"
    a.inner_text.return_value = "Decision X"
    page.query_selector_all.return_value = [a]
    page.content.return_value = "<html/>"

    sub_pages = ["https://main.knesset.gov.il/year/24", "https://main.knesset.gov.il/year/25"]
    cfg = ScrapeConfig(
        page_url="https://main.knesset.gov.il/about",
        anchor_selector="a",
        store=store,
        key=_KEY,
        sub_index_extractor=lambda html: sub_pages,
    )
    rows = scrape_pdf_index(cfg)
    assert len(rows) == 1


def test_scrape_pdf_index_raises_on_empty_with_existing_populated_store(
    tmp_path: Path, fake_playwright,
):
    store = LocalFsStore(tmp_path)
    page = fake_playwright["page"]
    # Pre-seed the store with a populated CSV.
    _atomic_write_csv(store, _KEY, [PdfRow(url="https://x.pdf", title="t", filename="f.pdf")])

    page.query_selector_all.return_value = []  # zero rows scraped
    page.content.return_value = "<html/>"

    cfg = ScrapeConfig(
        page_url="https://main.knesset.gov.il/x",
        store=store,
        key=_KEY,
    )
    with pytest.raises(EmptyUpstreamIndex):
        scrape_pdf_index(cfg)
    # Store must be untouched (not overwritten with empty contents).
    text = store.get_bytes(_KEY).decode("utf-8")
    loaded = list(csv.DictReader(io.StringIO(text)))
    assert len(loaded) == 1
