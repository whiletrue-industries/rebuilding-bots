"""Unit tests for scripts.insert_prompt_section.parse_single_section.

The DB upsert path is integration-style (requires an Aurora-compatible
postgres) and is exercised by the local docker E2E flow rather than by
unit tests; here we only lock the parsing contract so refactors don't
silently change how SECTION_KEY blocks are parsed.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

# Make scripts/ importable as a package-relative module.
SCRIPTS_DIR = Path(__file__).resolve().parent.parent / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

import insert_prompt_section  # noqa: E402


def test_parses_section_key_header_and_body():
    blob = (
        "<!-- SECTION_KEY: plenary_schedule -->\n"
        "<!-- header text here -->\n"
        "body line 1\n"
        "body line 2\n"
    )
    key, header, body = insert_prompt_section.parse_single_section(blob)
    assert key == "plenary_schedule"
    assert header == "header text here"
    assert body == "body line 1\nbody line 2"


def test_section_without_header_returns_none_header():
    blob = (
        "<!-- SECTION_KEY: x -->\n"
        "actual body without header comment\n"
    )
    key, header, body = insert_prompt_section.parse_single_section(blob)
    assert key == "x"
    assert header is None
    assert body == "actual body without header comment"


def test_strips_trailing_horizontal_rule():
    blob = (
        "<!-- SECTION_KEY: x -->\n"
        "body\n"
        "\n"
        "---\n"
    )
    _, _, body = insert_prompt_section.parse_single_section(blob)
    assert body == "body"


def test_zero_markers_raises():
    with pytest.raises(ValueError, match="expected exactly one"):
        insert_prompt_section.parse_single_section("just plain text\n")


def test_two_markers_raises():
    blob = (
        "<!-- SECTION_KEY: a -->\nbody a\n"
        "<!-- SECTION_KEY: b -->\nbody b\n"
    )
    with pytest.raises(ValueError, match="expected exactly one"):
        insert_prompt_section.parse_single_section(blob)


def test_real_plenary_schedule_section_parses():
    """The committed prompt_sections/plenary_schedule.md file parses cleanly."""
    path = (
        Path(__file__).resolve().parent.parent
        / "specs" / "unified" / "prompt_sections" / "plenary_schedule.md"
    )
    blob = path.read_text(encoding="utf-8")
    key, header, body = insert_prompt_section.parse_single_section(blob)
    assert key == "plenary_schedule"
    assert header is not None
    assert "search_unified__plenary_schedule" in body
    assert "לוח מליאה" in body or 'לו"ז' in body
