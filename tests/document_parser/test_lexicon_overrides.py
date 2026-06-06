"""Unit tests for lexicon._load_section_overrides candidate resolution.

After CLUSTER G the only filesystem candidate is the in-repo
specs/unified/extraction path; the legacy /srv/specs EFS fallback is gone
(overrides are served from the ArtifactStore seed/ prefix in deployed envs).
"""
import json
from pathlib import Path

from botnim.document_parser.lexicon import lexicon


def test_in_repo_override_is_loaded(monkeypatch, tmp_path):
    """The in-repo specs/unified/extraction candidate is read and parsed."""
    fake_module = tmp_path / "a" / "b" / "c" / "lexicon.py"
    fake_module.parent.mkdir(parents=True)
    fake_module.write_text("# fake", encoding="utf-8")

    overrides_dir = tmp_path / "specs" / "unified" / "extraction"
    overrides_dir.mkdir(parents=True)
    (overrides_dir / lexicon._OVERRIDES_FILENAME).write_text(
        json.dumps({"http://example/lex/1": "http://wiki/section-1"}),
        encoding="utf-8",
    )

    monkeypatch.setattr(lexicon, "__file__", str(fake_module))
    result = lexicon._load_section_overrides()
    assert result == {"http://example/lex/1": "http://wiki/section-1"}


def test_no_efs_srv_specs_candidate(monkeypatch, tmp_path):
    """The /srv/specs EFS fallback must NOT be consulted.

    We point the in-repo candidate at a non-existent tree and assert the
    loader returns {} — proving it does not silently fall through to a
    hardcoded /srv/specs path. Guarded by asserting open() is never called
    with a /srv/specs path.
    """
    fake_module = tmp_path / "x" / "y" / "z" / "lexicon.py"
    fake_module.parent.mkdir(parents=True)
    fake_module.write_text("# fake", encoding="utf-8")
    monkeypatch.setattr(lexicon, "__file__", str(fake_module))

    real_open = open

    def guard_open(path, *args, **kwargs):
        assert "/srv/specs" not in str(path), (
            f"_load_section_overrides probed forbidden EFS path: {path}"
        )
        return real_open(path, *args, **kwargs)

    monkeypatch.setattr("builtins.open", guard_open)
    assert lexicon._load_section_overrides() == {}


def test_source_overrides_filename_is_committed_constant():
    """Sanity: the filename constant the loader uses is unchanged."""
    assert lexicon._OVERRIDES_FILENAME == "lexicon_section_overrides.json"
