from __future__ import annotations

from pathlib import Path

import pytest

from botnim.sanity import gold_set


FIXTURES = Path(__file__).parent / "fixtures"


def test_load_from_env_var(monkeypatch):
    monkeypatch.setenv("BOTNIM_GOLD_SET_PATH", str(FIXTURES / "gold-set-mini.json"))
    entries = gold_set.load_gold_set()
    assert len(entries) == 2
    assert entries[0].row == 0
    assert entries[0].question.startswith("מה")
    assert entries[0].must_not_contain == ["איזה סוג מידע אתם מחפשים"]
    assert entries[1].observed_notes == ""


def test_missing_file_raises(monkeypatch, tmp_path):
    monkeypatch.setenv("BOTNIM_GOLD_SET_PATH", str(tmp_path / "nope.json"))
    with pytest.raises(FileNotFoundError):
        gold_set.load_gold_set()


def test_malformed_entry_raises(monkeypatch, tmp_path):
    bad = tmp_path / "bad.json"
    bad.write_text('[{"row": 0}]')  # missing required fields
    monkeypatch.setenv("BOTNIM_GOLD_SET_PATH", str(bad))
    with pytest.raises(ValueError):
        gold_set.load_gold_set()
