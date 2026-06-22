# botnim/document_parser/wikisource_law_book/process.py
"""fap driver: discover the law-book corpus, then extract each item through the
existing WikitextProcessor (one *_structure_content.json per law under
extraction/law_book/). Reusing WikitextProcessor is mandatory — it writes the
html_sha256 + extractor-version metadata that makes re-faps cheap (cache skip).
"""
import time
from pathlib import Path

from ...config import get_logger
from ..wikitext.pipeline_config import Environment, WikitextProcessorConfig
from ..wikitext.process_document import WikitextProcessor
from .enumerate_laws import discover_law_pages
from .manifest import LawBookEntry, read_manifest, write_manifest

logger = get_logger(__name__)

LAW_BOOK_SUBDIR = "law_book"


def process_law_book_source(environment: str, config_dir: Path, *,
                            include_regulations: bool = True,
                            min_expected_laws: int = 200,
                            apply_skip_list: bool = True,
                            rate_limit_seconds: float = 0.3,
                            **_ignored) -> None:
    config_dir = Path(config_dir)
    out_dir = config_dir / "extraction" / LAW_BOOK_SUBDIR
    manifest_path = out_dir / "manifest.csv"

    prior = read_manifest(manifest_path)
    entries = discover_law_pages(
        config_dir, include_regulations=include_regulations,
        min_expected_laws=min_expected_laws, apply_skip_list=apply_skip_list,
        prior=prior or None,
    )
    # Persist the discovered set (all 'pending') BEFORE extraction so a crash
    # mid-corpus still leaves a reviewable manifest.
    write_manifest(manifest_path, entries)

    ok = failed = 0
    for i, entry in enumerate(entries):
        try:
            cfg = WikitextProcessorConfig(
                input_url=entry.url, output_base_dir=out_dir,
                content_type="סעיף", environment=Environment(environment),
                model="gpt-4.1-mini", max_tokens=None,
            )
            entry.status = "ok" if WikitextProcessor(cfg).run(generate_markdown=False) else "failed"
        except Exception as e:  # per-item isolation — one bad page never aborts the corpus
            logger.warning("LAW_BOOK_ITEM_FAILED title=%s url=%s err=%s: %s",
                           entry.title, entry.url, type(e).__name__, e)
            entry.status = "failed"
        ok += entry.status == "ok"
        failed += entry.status == "failed"
        if (i + 1) % 25 == 0:
            write_manifest(manifest_path, entries)   # checkpoint progress
        if rate_limit_seconds:
            time.sleep(rate_limit_seconds)

    write_manifest(manifest_path, entries)
    logger.info("LAW_BOOK_DONE total=%d ok=%d failed=%d", len(entries), ok, failed)
