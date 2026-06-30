# tests/test_config_israeli_laws.py
from pathlib import Path
import yaml
from botnim.config import SPECS


def test_israeli_laws_context_registered():
    cfg = yaml.safe_load((SPECS / "unified" / "config.yaml").read_text(encoding="utf-8"))
    ctx = {c["slug"]: c for c in cfg["context"]}
    assert "israeli_laws" in ctx
    il = ctx["israeli_laws"]
    assert il.get("use_lexical_search") is True
    assert il.get("lexical_strategy") == "trigram"
    srcs = il["sources"]
    assert any(s.get("type") == "split" and "*" in s.get("source", "") for s in srcs)
    fetcher = next(s["fetcher"] for s in srcs if s.get("fetcher"))
    assert fetcher["kind"] == "wikisource_law_book"
