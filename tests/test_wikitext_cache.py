"""Wikitext per-source structure-extraction cache.

The WikitextProcessor downloads Wikisource HTML and runs a single
`gpt-4.1-mini` call to derive a hierarchical structure. With ~10 wikitext
sources on the unified bot, the daily fap burns ~10 LLM calls/day on
content that rarely changes. The cache fast-path skips the LLM call when
the ArtifactStore holds a matching object for the current html_sha256 +
WIKITEXT_EXTRACTOR_VERSION.  A version bump or HTML change → new key →
automatic miss, no explicit invalidation.

These tests exercise the cache decision logic without making real HTTP
requests or LLM calls.
"""
from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


def _build_processor(tmp_path: Path, html_bytes: bytes, *, model: str = "gpt-4.1-mini", bot: str = "unified"):
    """Construct a WikitextProcessor with mocked HTTP fetch + writable output."""
    from botnim.document_parser.wikitext.pipeline_config import (
        Environment,
        WikitextProcessorConfig,
    )
    from botnim.document_parser.wikitext.process_document import WikitextProcessor

    fake_resp = MagicMock()
    fake_resp.content = html_bytes
    with patch(
        "botnim.document_parser.wikitext.pipeline_config.requests.get",
        return_value=fake_resp,
    ):
        config = WikitextProcessorConfig(
            input_url="https://he.wikisource.org/wiki/Test_Page",
            output_base_dir=tmp_path,
            bot=bot,
            content_type="סעיף",
            environment=Environment.STAGING,
            model=model,
            max_tokens=None,
        )
    return WikitextProcessor(config)


def _make_store(tmp_path: Path):
    """A LocalFsStore rooted under tmp_path/store, simulating S3/EFS-independent
    durable storage."""
    from botnim.storage import LocalFsStore

    root = tmp_path / "store"
    root.mkdir(parents=True, exist_ok=True)
    return LocalFsStore(root)


def _cache_key(bot: str, html_sha256: str, version: str) -> str:
    return f"cache/wikitext/{bot}/{html_sha256}__{version}.json"


def _content_payload(*, html_sha256: str, version: str, model: str = "gpt-4.1-mini") -> bytes:
    payload = {
        "metadata": {
            "input_file": "https://he.wikisource.org/wiki/Test_Page",
            "document_name": "Test_Page",
            "environment": "staging",
            "model": model,
            "max_tokens": None,
            "total_items": 0,
            "structure_type": "nested_hierarchy",
            "mark_type": "סעיף",
            "html_sha256": html_sha256,
            "wikitext_extractor_version": version,
        },
        "structure": [],
    }
    return json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")


# Task 18 — config carries bot slug
def test_config_carries_bot_slug(tmp_path: Path):
    """WikitextProcessorConfig accepts and stores a `bot` slug used to
    namespace the durable wikitext cache key cache/wikitext/<bot>/..."""
    from unittest.mock import MagicMock, patch

    from botnim.document_parser.wikitext.pipeline_config import (
        Environment,
        WikitextProcessorConfig,
    )

    fake_resp = MagicMock()
    fake_resp.content = b"<html><body>x</body></html>"
    with patch(
        "botnim.document_parser.wikitext.pipeline_config.requests.get",
        return_value=fake_resp,
    ):
        config = WikitextProcessorConfig(
            input_url="https://he.wikisource.org/wiki/Test_Page",
            output_base_dir=tmp_path,
            bot="unified",
            content_type="סעיף",
            environment=Environment.STAGING,
            model="gpt-4.1-mini",
            max_tokens=None,
        )
    assert config.bot == "unified"


# Task 19 — store-backed cache hit
def test_run_cache_hit_from_store_skips_llm_call(tmp_path: Path):
    """A store object at the cache key with matching html_sha256 + version →
    skip the LLM. Requires NO local content_file on disk."""
    from botnim.document_parser.wikitext.process_document import (
        WIKITEXT_EXTRACTOR_VERSION,
    )

    html = b"<html><body>same content</body></html>"
    proc = _build_processor(tmp_path, html, bot="unified")
    store = _make_store(tmp_path)
    store.put_atomic(
        _cache_key("unified", proc.config.input_html_sha256, WIKITEXT_EXTRACTOR_VERSION),
        _content_payload(
            html_sha256=proc.config.input_html_sha256,
            version=WIKITEXT_EXTRACTOR_VERSION,
        ),
    )
    # Cache HIT must NOT depend on a local content_file.
    assert not proc.config.content_file.exists()

    llm = MagicMock()
    with patch(
        "botnim.document_parser.wikitext.process_document.get_artifact_store",
        return_value=store,
    ), patch(
        "botnim.document_parser.wikitext.process_document.extract_structure_from_html",
        llm,
    ):
        ok = proc.run(generate_markdown=False)

    assert ok is True
    llm.assert_not_called()


# Task 20 — adapted miss / write tests

def test_run_cache_miss_when_no_store_object(tmp_path: Path):
    """No store object at the cache key → fresh extraction."""
    html = b"<html><body>brand new</body></html>"
    proc = _build_processor(tmp_path, html)
    store = _make_store(tmp_path)  # empty store
    assert not proc.config.content_file.exists()

    def _fake_content_write(**kwargs):
        out = Path(kwargs["output_path"])
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text("{}", encoding="utf-8")

    with patch(
        "botnim.document_parser.wikitext.process_document.get_artifact_store",
        return_value=store,
    ), patch(
        "botnim.document_parser.wikitext.process_document.extract_structure_from_html",
        return_value=[],
    ) as llm, patch(
        "botnim.document_parser.wikitext.process_document.extract_content_from_html",
        side_effect=_fake_content_write,
    ):
        ok = proc.run(generate_markdown=False)

    assert ok is True
    llm.assert_called_once()


def test_run_cache_miss_when_html_changed(tmp_path: Path):
    """Store has an object only under a stale html_sha256 key → fresh extraction."""
    from botnim.document_parser.wikitext.process_document import (
        WIKITEXT_EXTRACTOR_VERSION,
    )

    html = b"<html><body>new content</body></html>"
    proc = _build_processor(tmp_path, html, bot="unified")
    store = _make_store(tmp_path)
    # Object stored under a mismatched hash → current run's key won't exist.
    store.put_atomic(
        _cache_key("unified", "0" * 64, WIKITEXT_EXTRACTOR_VERSION),
        _content_payload(html_sha256="0" * 64, version=WIKITEXT_EXTRACTOR_VERSION),
    )

    def _fake_content_write(**kwargs):
        out = Path(kwargs["output_path"])
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text("{}", encoding="utf-8")

    with patch(
        "botnim.document_parser.wikitext.process_document.get_artifact_store",
        return_value=store,
    ), patch(
        "botnim.document_parser.wikitext.process_document.extract_structure_from_html",
        return_value=[],
    ) as llm, patch(
        "botnim.document_parser.wikitext.process_document.extract_content_from_html",
        side_effect=_fake_content_write,
    ):
        proc.run(generate_markdown=False)

    llm.assert_called_once()


def test_run_cache_miss_when_extractor_version_bumped(tmp_path: Path):
    """Store object lives under an older-version key → current key misses → fresh extraction."""
    html = b"<html><body>same content</body></html>"
    proc = _build_processor(tmp_path, html, bot="unified")
    store = _make_store(tmp_path)
    store.put_atomic(
        _cache_key("unified", proc.config.input_html_sha256, "v0-some-older-version"),
        _content_payload(
            html_sha256=proc.config.input_html_sha256,
            version="v0-some-older-version",
        ),
    )

    def _fake_content_write(**kwargs):
        out = Path(kwargs["output_path"])
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text("{}", encoding="utf-8")

    with patch(
        "botnim.document_parser.wikitext.process_document.get_artifact_store",
        return_value=store,
    ), patch(
        "botnim.document_parser.wikitext.process_document.extract_structure_from_html",
        return_value=[],
    ) as llm, patch(
        "botnim.document_parser.wikitext.process_document.extract_content_from_html",
        side_effect=_fake_content_write,
    ):
        proc.run(generate_markdown=False)

    llm.assert_called_once()


def test_stage_one_output_stamps_html_sha256_and_version(tmp_path: Path):
    """Stage 1 writes html_sha256 + wikitext_extractor_version into metadata
    so subsequent runs can compare against it."""
    from botnim.document_parser.wikitext.process_document import (
        WIKITEXT_EXTRACTOR_VERSION,
    )

    html = b"<html><body>fresh</body></html>"
    proc = _build_processor(tmp_path, html)
    store = _make_store(tmp_path)
    expected_hash = proc.config.input_html_sha256

    def _fake_content_write(**kwargs):
        out = Path(kwargs["output_path"])
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text("{}", encoding="utf-8")

    with patch(
        "botnim.document_parser.wikitext.process_document.get_artifact_store",
        return_value=store,
    ), patch(
        "botnim.document_parser.wikitext.process_document.extract_structure_from_html",
        return_value=[],
    ), patch(
        "botnim.document_parser.wikitext.process_document.build_nested_structure",
        return_value=[],
    ), patch(
        "botnim.document_parser.wikitext.process_document.extract_content_from_html",
        side_effect=_fake_content_write,
    ):
        proc.run(generate_markdown=False)

    # Stage 1 writes the structure_file with the cache key in metadata.
    assert proc.config.structure_file.exists()
    with open(proc.config.structure_file, "r", encoding="utf-8") as f:
        written = json.load(f)
    md = written["metadata"]
    assert md["html_sha256"] == expected_hash
    assert md["wikitext_extractor_version"] == WIKITEXT_EXTRACTOR_VERSION


# Task 21 — durability test: write cache via one store, HIT from a FRESH store instance

def test_durable_cache_hit_across_fresh_store_instance(tmp_path: Path, monkeypatch):
    """Run #1 writes the cache object to S3 via one S3Store; Run #2 — a fresh
    processor with no local content_file and a brand-new S3Store instance
    (simulating a new container) — HITs purely from S3 and makes ZERO LLM
    calls."""
    import boto3
    from moto import mock_aws

    from botnim.document_parser.wikitext.process_document import (
        WIKITEXT_EXTRACTOR_VERSION,
        WikitextProcessor,
    )
    from botnim.document_parser.wikitext.pipeline_config import (
        Environment,
        WikitextProcessorConfig,
    )

    html = b"<html><body>durable same content</body></html>"

    def _fake_content_write(**kwargs):
        # Minimal valid content_file carrying the cache key in metadata, so
        # run() uploads it and the next run can validate the HIT.
        out = Path(kwargs["output_path"])
        out.parent.mkdir(parents=True, exist_ok=True)
        import hashlib
        sha = hashlib.sha256(html).hexdigest()
        out.write_bytes(
            _content_payload(html_sha256=sha, version=WIKITEXT_EXTRACTOR_VERSION)
        )

    with mock_aws():
        monkeypatch.setenv("AWS_DEFAULT_REGION", "il-central-1")
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
        s3 = boto3.client("s3", region_name="il-central-1")
        s3.create_bucket(
            Bucket="botnim-artifacts-test",
            CreateBucketConfiguration={"LocationConstraint": "il-central-1"},
        )

        from botnim.storage import S3Store

        # ----- Run #1: fresh container, cold cache, performs extraction -----
        fake_resp = MagicMock()
        fake_resp.content = html
        with patch(
            "botnim.document_parser.wikitext.pipeline_config.requests.get",
            return_value=fake_resp,
        ):
            cfg1 = WikitextProcessorConfig(
                input_url="https://he.wikisource.org/wiki/Durable_Page",
                output_base_dir=tmp_path / "run1",
                bot="unified",
                content_type="סעיף",
                environment=Environment.STAGING,
                model="gpt-4.1-mini",
                max_tokens=None,
            )
        proc1 = WikitextProcessor(cfg1)
        sha = proc1.config.input_html_sha256

        store1 = S3Store("botnim-artifacts-test")
        llm1 = MagicMock(return_value=[])
        with patch(
            "botnim.document_parser.wikitext.process_document.get_artifact_store",
            return_value=store1,
        ), patch(
            "botnim.document_parser.wikitext.process_document.extract_structure_from_html",
            llm1,
        ), patch(
            "botnim.document_parser.wikitext.process_document.build_nested_structure",
            return_value=[],
        ), patch(
            "botnim.document_parser.wikitext.process_document.extract_content_from_html",
            side_effect=_fake_content_write,
        ):
            ok1 = proc1.run(generate_markdown=False)
        assert ok1 is True
        llm1.assert_called_once()  # cold cache → one extraction

        # The cache object now lives in S3 under the versioned key.
        expected_key = _cache_key("unified", sha, WIKITEXT_EXTRACTOR_VERSION)
        assert store1.exists(expected_key)

        # ----- Run #2: brand-new container — fresh processor, fresh store,
        # NO local content_file from run #1 (different output dir) -----
        with patch(
            "botnim.document_parser.wikitext.pipeline_config.requests.get",
            return_value=fake_resp,
        ):
            cfg2 = WikitextProcessorConfig(
                input_url="https://he.wikisource.org/wiki/Durable_Page",
                output_base_dir=tmp_path / "run2",
                bot="unified",
                content_type="סעיף",
                environment=Environment.STAGING,
                model="gpt-4.1-mini",
                max_tokens=None,
            )
        proc2 = WikitextProcessor(cfg2)
        assert proc2.config.input_html_sha256 == sha
        assert not proc2.config.content_file.exists()  # no local state

        store2 = S3Store("botnim-artifacts-test")  # FRESH instance
        llm2 = MagicMock()
        with patch(
            "botnim.document_parser.wikitext.process_document.get_artifact_store",
            return_value=store2,
        ), patch(
            "botnim.document_parser.wikitext.process_document.extract_structure_from_html",
            llm2,
        ):
            ok2 = proc2.run(generate_markdown=False)

        assert ok2 is True
        llm2.assert_not_called()  # durable HIT → ZERO LLM calls
