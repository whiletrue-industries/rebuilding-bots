"""Knesset Lexicon scraper.

Fetches the index page at ``main.knesset.gov.il/about/lexicon/pages/default.aspx``
and follows each link to extract Hebrew biographies / term definitions.

Index-hash short-circuit (added 2026-05-06): each run computes a sha256
of the index-page HTML and compares to a sentinel stored next to the
output CSV. If the index is unchanged AND the CSV exists, the per-entry
scrape (~700 entries × 5s sleep ≈ 1 hour) is skipped entirely. The
sentinel is updated only after a successful re-scrape, so a partial /
crashed run does not poison future runs into thinking they're up-to-date.

Catches: added / removed / renamed entries (the common case).
Misses: content edits *within* an existing entry (rare; the daily
Lambda picks up such edits on the next index-page change, since most
edits coincide with link-list churn).
"""
import hashlib
import io
import json
from pathlib import Path

import requests
from pyquery import PyQuery as pq
import csv
import time

from ...storage.base import ArtifactStore, seed_key
from ...storage.csv_writer import write_csv_artifact

headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:136.0) Gecko/20100101 Firefox/136.0',
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'en-US,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate, br, zstd',
    'Content-Type': 'application/json',
    'Connection': 'keep-alive',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
    'Priority': 'u=0',
    'Cookie': 'WSS_FullScreenMode=false; ASP.NET_SessionId=eqvbp1ldaamr2ifdm1acoryq; rbzid=ibrrtGNUvwcQR0B+ZmAUv5JGm+j+IaI1yldtf+VFdZkE9NdGbcNYjW5GliBjxNc+6WlokT2xDq3opFrotCw/6Ka+8WzsK7g1mJpj4TUV3pQyMyWIguRz6Pc//kqNsPVDbK0XurV8mZk3v50DY66CxaPvQrGsrp3WiA6kDiYzNzRtXQwXAg/xTgfZwDK4S2KOK+sP21Hj1S3vCtEAYOQhMFfnsC/s; waap_id=Sv+smIxDLlwfUf8YD6dk4X2CWTCWc5MgEBSjlz6EQZJ7V5GEMnwXRH7YXvsBhNd5ngO6td26kDnaVng18uVTjk567BcQyIO3BgaPTa7YfbDaeYB4lLXGJCcSsLEZN7z2pn3g+BOezbq++iezBbyLL/jiMNxdp8CV9j5tHEyMN7m9tyyzv/6Rcf/OgM+G3UTZLLgMutB2tZhM/olJxAHVYUUE7SXJ; deviceChannel=Default'
}

BASE = 'https://main.knesset.gov.il'
URL = f'{BASE}/about/lexicon/pages/default.aspx'
LINK_CLASS = 'td.lexColumns a'
CONTENT_CLASS = '.LexiconContent'

# Sentinel suffix appended to the output CSV path. Stored sha256 hex.
SENTINEL_SUFFIX = '.index.sha256'

# Columns the current scraper emits. The set is checked against the
# existing CSV header so an upgrade from a legacy 1-column CSV forces
# a re-scrape even when the sentinel still matches Knesset's index.
_CURRENT_CSV_FIELDNAMES = ('מידע', 'lexicon_url', 'source_url')

# Hand-curated mapping from Knesset lexicon URL → Wikisource section
# anchor. Covers entries whose body describes a takanon/law section
# WITHOUT naming the section number explicitly — so ``derive_section_url``
# can't catch it. See
# ``specs/unified/extraction/lexicon_section_overrides.json``.
_OVERRIDES_FILENAME = 'lexicon_section_overrides.json'


def _load_section_overrides(
    store=None,
    bot: str = 'unified',
    _disk_candidates=None,
) -> dict[str, str]:
    """Load hand-curated lexicon_url → wikisource_url overrides.

    Resolution order:
      1. ``seed/<bot>/lexicon_section_overrides.json`` via ``store`` (the
         operator-owned immutable seed).  A missing object falls through.
      2. The in-repo ``specs/unified/extraction/lexicon_section_overrides.json``
         resolved relative to this module (for in-repo / local-dev invocations).

    In deployed environments the same file is served from the ArtifactStore
    ``seed/`` prefix, so there is no longer a hardcoded ``/srv/specs``
    filesystem fallback. Returns ``{}`` on any read/parse error so the
    scraper degrades to the derive-or-fallback behaviour.
    """
    def _coerce(data) -> dict[str, str] | None:
        if isinstance(data, dict):
            return {k: v for k, v in data.items() if isinstance(v, str) and v}
        return None

    if store is not None:
        try:
            raw = store.get_bytes(seed_key(bot, _OVERRIDES_FILENAME))
            coerced = _coerce(json.loads(raw.decode('utf-8')))
            if coerced is not None:
                return coerced
        except (FileNotFoundError, json.JSONDecodeError, UnicodeDecodeError):
            pass

    candidates = _disk_candidates if _disk_candidates is not None else [
        Path(__file__).resolve().parents[3]
            / 'specs' / 'unified' / 'extraction' / _OVERRIDES_FILENAME,
    ]
    for p in candidates:
        try:
            with open(p, encoding='utf-8') as f:
                coerced = _coerce(json.load(f))
                if coerced is not None:
                    return coerced
        except (OSError, json.JSONDecodeError):
            continue
    return {}


def _csv_matches_current_schema(store: ArtifactStore, key: str) -> bool:
    """True iff an object exists at ``key`` and its header is the current
    3-column schema. Legacy 1-column CSVs return False so the scraper
    re-scrapes into the new shape."""
    if not store.exists(key):
        return False
    text = store.get_bytes(key).decode("utf-8")
    reader = csv.reader(io.StringIO(text))
    try:
        header = next(reader)
    except StopIteration:
        return False
    return tuple(header) == _CURRENT_CSV_FIELDNAMES


def _fetch_index() -> tuple[str, str]:
    """Fetch the lexicon index page. Returns (html, content_sha256_hex).

    The hash is computed over a *dehydrated* form — sorted unique entry
    hrefs joined by newlines — NOT the raw HTML. Knesset's ASP.NET emits
    per-request ``__VIEWSTATE`` and other session-coupled tokens, so two
    consecutive raw-HTML hashes never match even when the entry list is
    unchanged. Hashing the link list catches added/removed/renamed
    entries (which is what we care about) and survives ViewState drift.
    """
    response = requests.get(URL, headers=headers)
    if response.status_code != 200:
        print(response.text)
        raise Exception(f"Failed to load page: {response.status_code}")
    doc = pq(response.text)
    hrefs = sorted({(pq(a).attr('href') or '').strip() for a in doc(LINK_CLASS)})
    hrefs = [h for h in hrefs if h]
    fingerprint = '\n'.join(hrefs)
    digest = hashlib.sha256(fingerprint.encode('utf-8')).hexdigest()
    return response.text, digest


def _iter_entries(index_html: str):
    """Iterate links from already-fetched index HTML, yield content per entry."""
    doc = pq(index_html)
    links = doc(LINK_CLASS)

    for link in links:
        href = pq(link).attr('href')
        print('LINK', href)
        link_text = pq(link).text()
        print('ITEM', link_text)
        if href:
            content_url = BASE + href
            content_response = requests.get(content_url, headers=headers)
            if content_response.status_code == 200:
                text = content_response.text
                content_doc = pq(text)
                content = content_doc(CONTENT_CLASS).text()
                content = content.replace('תוכן דף', '').strip()
                print('CONTENT', content)
                yield {
                    'link_text': link_text,
                    'content_url': content_url,
                    'content': content
                }
                time.sleep(5)  # Respectful scraping delay
            else:
                print(f"Failed to load content from {content_url}: {content_response.status_code}")


def scrape():
    """Backwards-compat generator: fetch + iterate. No change-detection."""
    index_html, _ = _fetch_index()
    yield from _iter_entries(index_html)


def scrape_lexicon(*, store: ArtifactStore, key: str):
    """Scrape the Knesset lexicon to a CSV artifact, with index-hash short-circuit.

    Output CSV columns:
      - ``מידע``        : the lexicon entry body. No embedded markdown link.
      - ``lexicon_url`` : the original Knesset Lexicon page URL (traceability).
      - ``source_url``  : a Wikisource section anchor. Priority order:
                          (1) hand-curated overrides file
                              (``lexicon_section_overrides.json``),
                          (2) regex-derived anchor when the body cites
                              "סעיף N לחוק/לתקנון" explicitly,
                          (3) fall back to the Lexicon URL itself.

    Writes the CSV at ``key`` and the sentinel at ``key + '.index.sha256'``
    through the artifact store. Sentinel is written AFTER the CSV so a crash
    mid-scrape leaves the old sentinel + old CSV untouched.
    """
    from .section_url import derive_section_url

    sentinel_key = key + SENTINEL_SUFFIX

    index_html, new_hash = _fetch_index()

    if store.exists(sentinel_key) and store.exists(key) and _csv_matches_current_schema(store, key):
        try:
            old_hash = store.get_bytes(sentinel_key).decode("utf-8").strip()
        except Exception:
            old_hash = ''
        if old_hash == new_hash:
            print(f"lexicon: index unchanged (sha={new_hash[:12]}); leaving {key} as-is")
            return

    state = 'changed' if store.exists(sentinel_key) else 'first run'
    if store.exists(sentinel_key) and store.exists(key) and not _csv_matches_current_schema(store, key):
        state = 'schema upgrade'
    overrides = _load_section_overrides(store=store, bot='unified')
    print(f"lexicon: index {state} (sha={new_hash[:12]}); scraping all entries... ({len(overrides)} curated overrides)")
    rows: list[dict[str, str]] = []
    for entry in _iter_entries(index_html):
        link_text = entry.get('link_text', '') or ''
        content = entry.get('content', '') or ''
        content_url = entry.get('content_url', '') or ''
        # rstrip trailing periods before appending our own so we don't emit
        # ".." when ``content`` already ends with a period (most glossary
        # entries do — the upstream scrape leaves a sentence-final dot).
        body = f"{link_text}: {content}".rstrip(".") + "."
        haystack = f"{link_text}\n{content}"
        # Source-URL priority: curated override > regex-derived > lexicon page.
        source_url = (
            overrides.get(content_url)
            or derive_section_url(haystack)
            or content_url
        )
        rows.append({
            'מידע':        body,
            'lexicon_url': content_url,
            'source_url':  source_url,
        })

    write_csv_artifact(
        store, key, rows,
        fieldnames=['מידע', 'lexicon_url', 'source_url'],
    )

    # Write sentinel only after CSV write succeeds.
    store.put_atomic(sentinel_key, new_hash.encode("utf-8"))
