"""Derive the set of laws already covered by the `legal_text` context so the
bulk law-book ingestion can skip them (no double-indexing). Sourced from
config.yaml at runtime so it can never drift from reality.
"""
from pathlib import Path
from urllib.parse import unquote
import yaml


def title_from_url(url: str) -> str:
    """`.../wiki/%D7%97%D7%95%D7%A7_%D7%94%D7%9B%D7%A0%D7%A1%D7%AA` -> `חוק הכנסת`."""
    tail = unquote(url).split("/wiki/")[-1]
    return tail.replace("_", " ").strip()


def legal_text_skip_titles(config_dir: Path) -> set[str]:
    cfg = yaml.safe_load((Path(config_dir) / "config.yaml").read_text(encoding="utf-8"))
    titles: set[str] = set()
    for ctx in cfg.get("context", []):
        if ctx.get("slug") != "legal_text":
            continue
        for src in ctx.get("sources", []):
            fetcher = src.get("fetcher") or {}
            url = fetcher.get("input_url")
            if url:
                titles.add(title_from_url(url))
    return titles
