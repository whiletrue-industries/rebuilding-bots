"""Unit tests for validate_extracted_data — wrapper-dict unwrap behaviour.

Background: response_format={"type":"json_object"} forbids a top-level JSON
array, so the LLM wraps multi-item responses under a single key:
``{"decisions": [{...}, {...}]}``. Without the unwrap, the validator
treats the wrapper as the single item and every required field is "missing".
"""
from __future__ import annotations

from pathlib import Path

import pytest

from botnim.document_parser.pdfs.field_extraction import validate_extracted_data
from botnim.document_parser.pdfs.exceptions import ValidationError as PDFValidationError
from botnim.document_parser.pdfs.pdf_extraction_config import SourceConfig, FieldConfig


def _minimal_config() -> SourceConfig:
    """Smallest SourceConfig that exercises the schema path.

    We only need one required field for these tests — the unwrap behaviour
    is structural and doesn't depend on the field set.
    """
    return SourceConfig(
        output_csv_path=Path("/tmp/out.csv"),
        external_source_url="https://example.invalid/dummy",
        fields=[
            FieldConfig(name="title", description="doc title"),
        ],
    )


def _schema(required_fields: list[str]) -> dict:
    return {
        "type": "object",
        "properties": {name: {"type": "string"} for name in required_fields},
        "required": required_fields,
    }


def test_flat_dict_validates_as_single_item():
    """Single-item PDFs (the LLM returns a flat dict): no unwrap, pass-through."""
    cfg = _minimal_config()
    schema = _schema(["title"])
    data = {"title": "Single decision"}
    out = validate_extracted_data(data, schema, cfg)
    assert out == [{"title": "Single decision"}]


def test_list_of_dicts_validates_as_array():
    """Pre-existing path: LLM somehow returned a bare list (rare under
    json_object) — still validates each element."""
    cfg = _minimal_config()
    schema = _schema(["title"])
    data = [{"title": "A"}, {"title": "B"}]
    out = validate_extracted_data(data, schema, cfg)
    assert out == [{"title": "A"}, {"title": "B"}]


def test_single_key_wrapper_dict_is_unwrapped():
    """Multi-item PDFs (json_object forces wrap): the single-key wrapper
    around a list of dicts must be unwrapped, not validated as a unit."""
    cfg = _minimal_config()
    schema = _schema(["title"])
    data = {"decisions": [{"title": "A"}, {"title": "B"}]}
    out = validate_extracted_data(data, schema, cfg)
    assert out == [{"title": "A"}, {"title": "B"}]


def test_single_key_wrapper_with_non_dict_list_is_not_unwrapped():
    """Don't unwrap if the wrapper's value is a list of non-dicts — that's
    a different shape we don't want to swallow silently. Should raise."""
    cfg = _minimal_config()
    schema = _schema(["title"])
    data = {"items": ["just", "strings"]}
    with pytest.raises(PDFValidationError):
        validate_extracted_data(data, schema, cfg)


def test_multi_key_dict_is_not_unwrapped():
    """A dict with multiple keys must NOT be unwrapped — that's a single
    item that genuinely has multiple fields. Validate as one item."""
    cfg = _minimal_config()
    schema = _schema(["title"])
    # 'title' IS present, plus an extra 'summary' field. The validator
    # should treat this as a single item (NOT unwrap "summary" as a list)
    # and pass it through unchanged.
    data = {"title": "ok", "summary": "extra detail"}
    out = validate_extracted_data(data, schema, cfg)
    assert out == [{"title": "ok", "summary": "extra detail"}]
