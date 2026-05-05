# Knesset SharePoint scraper

First-party replacement for the BudgetKey datapackages
`knesset_legal_advisor`, `knesset_legal_advisor_letters`, and
`ethics_committee_decisions`. The BK pipelines have published zero-row
`index.csv` since 2025-01 (their headless-Chrome infra broke); this
module performs the equivalent scrape in-process using
`playwright` + `playwright-stealth`.

## When to use

- Daily refresh of `legal_advisor_opinions` and `legal_advisor_letters`
  contexts (working as of 2026-05-05; 75 + 159 rows respectively).
- `ethics_committee_decisions`: the upstream SharePoint page was
  redesigned and no longer contains the table-of-decisions structure
  the original BK scraper relied on. The preset config remains in this
  module so it'll just work if/when the page is restored, but operators
  should expect zero rows today.

## Live smoke

```python
from pathlib import Path
from botnim.document_parser.knesset_sharepoint import (
    legal_advisor_opinions_config, scrape_pdf_index,
)

cfg = legal_advisor_opinions_config(Path("/tmp/index.csv"))
rows = scrape_pdf_index(cfg)
print(f"got {len(rows)} rows; latest: {rows[0].title}")
```

## Deployment

The scraper requires:

- `pip install playwright playwright-stealth`
- `python -m playwright install chromium` (~150 MB image bloat)
- OS deps for Chromium (apt: `libgbm1 libdrm2 libxkbcommon0 libxcomposite1
  libxdamage1 libxrandr2 libgtk-3-0 libpango-1.0-0 libcairo2`)

To keep the base `botnim-api` image lean, the scraper is **not wired
into `fetch_and_process` automatically** in this PR. Two integration
options to choose from in a follow-up PR:

1. **Bootstrap-only** (recommended): operator runs the scraper manually
   from laptop / a one-shot ECS task; the resulting `index.csv` is
   uploaded to EFS so the existing `kind: pdf` fetcher reads it on
   every refresh. Same pattern as `gov_il_decisions` bootstrap.
2. **Always-on**: add `kind: knesset_sharepoint_pdf` fetcher that
   wraps `scrape_pdf_index` + downstream PDF processing. Adds Chromium
   to the deployed image.

See the module docstring for the API surface.
