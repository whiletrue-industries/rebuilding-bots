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
from pathlib import Path

import requests
from pyquery import PyQuery as pq
import csv
import time

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


def _fetch_index() -> tuple[str, str]:
    """Fetch the lexicon index page. Returns (html, sha256_hex)."""
    response = requests.get(URL, headers=headers)
    if response.status_code != 200:
        print(response.text)
        raise Exception(f"Failed to load page: {response.status_code}")
    digest = hashlib.sha256(response.text.encode('utf-8')).hexdigest()
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


def scrape_lexicon(output_path):
    """Scrape the Knesset lexicon to CSV, with index-hash short-circuit.

    On every run, fetches the index page and computes its sha256. If the
    hash matches the sentinel stored alongside ``output_path`` AND the CSV
    already exists, returns immediately without re-scraping. Otherwise
    runs the full per-entry scrape and writes both CSV + sentinel.

    The sentinel write happens AFTER the CSV write so a crash mid-scrape
    leaves the old sentinel (and old CSV) untouched, and the next run
    re-attempts.
    """
    output_path = Path(output_path)
    sentinel_path = output_path.parent / (output_path.name + SENTINEL_SUFFIX)

    index_html, new_hash = _fetch_index()

    if sentinel_path.exists() and output_path.exists():
        try:
            old_hash = sentinel_path.read_text(encoding='utf-8').strip()
        except OSError:
            old_hash = ''
        if old_hash == new_hash:
            print(f"lexicon: index unchanged (sha={new_hash[:12]}); leaving {output_path} as-is")
            return

    state = 'changed' if sentinel_path.exists() else 'first run'
    print(f"lexicon: index {state} (sha={new_hash[:12]}); scraping all entries...")
    rows = []
    for entry in _iter_entries(index_html):
        link_text = entry.get('link_text', '')
        content = entry.get('content', '')
        content_url = entry.get('content_url', '')
        formatted = f"{link_text}: {content}. \n\n[קישור למידע]({content_url})."
        rows.append([formatted])

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, 'w', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['מידע'])
        writer.writerows(rows)

    # Write sentinel only after CSV write succeeds.
    sentinel_path.write_text(new_hash, encoding='utf-8')
