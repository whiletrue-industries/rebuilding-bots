"""Tests for SourceConfig backwards-compatibility + new local_index_csv_path field."""
from __future__ import annotations

import pytest
from pathlib import Path
from pydantic import ValidationError

from botnim.document_parser.pdfs.pdf_extraction_config import (
    SourceConfig,
    FieldConfig,
)


def _fields():
    return [FieldConfig(name="x", description="y", example="z", hint="w")]


def test_external_source_url_only_still_valid():
    """Existing BK path: external_source_url set, local_index_csv_path unset."""
    cfg = SourceConfig(
        output_csv_path=Path("/tmp/out.csv"),
        fields=_fields(),
        external_source_url="https://next.obudget.org/datapackages/x/y",
    )
    assert cfg.external_source_url == "https://next.obudget.org/datapackages/x/y"
    assert cfg.local_index_csv_path is None


def test_local_index_csv_path_only_valid():
    """New local-index path: local_index_csv_path set, external_source_url unset."""
    cfg = SourceConfig(
        output_csv_path=Path("/tmp/out.csv"),
        fields=_fields(),
        local_index_csv_path="extraction/foo/index.csv",
    )
    assert cfg.local_index_csv_path == "extraction/foo/index.csv"
    assert cfg.external_source_url is None


def test_both_set_rejected():
    """Mutually exclusive — exactly one must be set."""
    with pytest.raises(ValidationError) as e:
        SourceConfig(
            output_csv_path=Path("/tmp/out.csv"),
            fields=_fields(),
            external_source_url="https://x",
            local_index_csv_path="y",
        )
    assert "exactly one" in str(e.value).lower()


def test_neither_set_rejected():
    """Mutually exclusive — exactly one must be set."""
    with pytest.raises(ValidationError) as e:
        SourceConfig(
            output_csv_path=Path("/tmp/out.csv"),
            fields=_fields(),
        )
    assert "exactly one" in str(e.value).lower()
