"""Tests for the hand-rolled markdown subset walker."""
from __future__ import annotations

import pytest
from botnim.word_doc.markdown_walker import parse_markdown


def test_plain_paragraphs_split_on_blank_line():
    blocks = parse_markdown("first paragraph\n\nsecond paragraph")
    assert len(blocks) == 2
    assert blocks[0]["type"] == "paragraph"
    assert blocks[0]["runs"][0]["text"] == "first paragraph"
    assert blocks[1]["type"] == "paragraph"


def test_heading_double_hash():
    blocks = parse_markdown("## subhead")
    assert blocks[0] == {"type": "heading", "level": 1, "text": "subhead"}


def test_heading_triple_hash():
    blocks = parse_markdown("### sub-subhead")
    assert blocks[0] == {"type": "heading", "level": 2, "text": "sub-subhead"}


def test_bullet_list_dash():
    blocks = parse_markdown("- one\n- two")
    assert blocks[0] == {"type": "list_item", "ordered": False, "runs": [{"text": "one", "bold": False, "italic": False, "url": None}]}
    assert blocks[1] == {"type": "list_item", "ordered": False, "runs": [{"text": "two", "bold": False, "italic": False, "url": None}]}


def test_bullet_list_asterisk():
    blocks = parse_markdown("* one")
    assert blocks[0]["ordered"] is False


def test_numbered_list():
    blocks = parse_markdown("1. one\n2. two")
    assert blocks[0]["ordered"] is True


def test_inline_bold():
    blocks = parse_markdown("hello **world** end")
    runs = blocks[0]["runs"]
    assert [r["text"] for r in runs] == ["hello ", "world", " end"]
    assert [r["bold"] for r in runs] == [False, True, False]


def test_inline_italic():
    blocks = parse_markdown("hello *world* end")
    runs = blocks[0]["runs"]
    assert [r["italic"] for r in runs] == [False, True, False]


def test_inline_link():
    blocks = parse_markdown("see [docs](https://example.com) for more")
    runs = blocks[0]["runs"]
    link_run = next(r for r in runs if r["url"])
    assert link_run["text"] == "docs"
    assert link_run["url"] == "https://example.com"


def test_hebrew_passthrough():
    blocks = parse_markdown("שלום עולם")
    assert blocks[0]["runs"][0]["text"] == "שלום עולם"
