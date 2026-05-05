"""First-party scraper for Knesset SharePoint pages behind Reblaze.

This module replaces the BudgetKey ``knesset_legal_advisor*`` and
``ethics_committee_decisions`` datapackages, which have been frozen
(publishing zero-row index.csv) since BK's headless-Chrome
infrastructure broke. We do the equivalent scrape in-process using
Playwright + stealth.

See ``scraper.py`` for the worker function and the README in this
directory for the integration story.
"""
from .scraper import (
    PdfRow,
    ScrapeConfig,
    scrape_pdf_index,
)

__all__ = ["PdfRow", "ScrapeConfig", "scrape_pdf_index"]
