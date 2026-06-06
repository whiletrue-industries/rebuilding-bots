"""Smoke test for scripts/upload_extraction_seed.py.

Verifies the argparse surface and that each seed file is uploaded to its
expected seed/<bot>/<relpath> key via store.put_atomic, using a LocalFsStore
backed by a temp specs root.

Key assertion: the keys the upload script writes to MUST exactly match the
keys the readers/fetchers read from (Tasks 22-24).
"""
from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

from botnim.storage import LocalFsStore

_SCRIPT = (
    Path(__file__).resolve().parents[1] / "scripts" / "upload_extraction_seed.py"
)


def _load_script():
    spec = importlib.util.spec_from_file_location("upload_extraction_seed", _SCRIPT)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _make_specs(root: Path) -> Path:
    """Create a minimal specs tree with all three seed files."""
    unified = root / "unified"
    (unified / "extraction" / "ethics_decisions").mkdir(parents=True)
    (unified / "extraction" / "ethics_decisions" / "index.csv").write_text(
        "url,filename,date,knesset_num,title\n"
        "https://main.knesset.gov.il/x.pdf,x.pdf,2008-01-01,17,test\n",
        encoding="utf-8",
    )
    (unified / "extraction" / "lexicon_section_overrides.json").write_text(
        '{"https://main.knesset.gov.il/About/Lexicon/Pages/query.aspx": "https://he.wikisource.org/wiki/test#1"}',
        encoding="utf-8",
    )
    (unified / "common-knowledge.md").write_text(
        "part A\n---\npart B", encoding="utf-8"
    )
    return root


def test_uploads_all_three_seed_files(tmp_path: Path):
    mod = _load_script()
    specs_root = _make_specs(tmp_path / "specs")
    store = LocalFsStore(tmp_path / "store")

    uploaded = mod.upload_seed(specs_root=specs_root, bot="unified", store=store)

    assert set(uploaded) == {
        "seed/unified/ethics_decisions/index.csv",
        "seed/unified/lexicon_section_overrides.json",
        "seed/unified/common-knowledge.md",
    }
    assert store.exists("seed/unified/ethics_decisions/index.csv")
    assert store.exists("seed/unified/lexicon_section_overrides.json")
    assert store.get_bytes("seed/unified/common-knowledge.md") == b"part A\n---\npart B"


def test_idempotent_reupload_overwrites(tmp_path: Path):
    mod = _load_script()
    specs_root = _make_specs(tmp_path / "specs")
    store = LocalFsStore(tmp_path / "store")

    mod.upload_seed(specs_root=specs_root, bot="unified", store=store)
    # Mutate one source and re-run; second run must overwrite (new version).
    (specs_root / "unified" / "common-knowledge.md").write_text(
        "updated content", encoding="utf-8"
    )
    mod.upload_seed(specs_root=specs_root, bot="unified", store=store)
    assert store.get_bytes("seed/unified/common-knowledge.md") == b"updated content"


def test_argparse_defaults():
    mod = _load_script()
    args = mod.parse_args([])
    assert args.bot == "unified"

    args2 = mod.parse_args(["--bot", "other"])
    assert args2.bot == "other"


def test_missing_source_raises(tmp_path: Path):
    mod = _load_script()
    specs_root = tmp_path / "specs"  # empty — no unified dir
    store = LocalFsStore(tmp_path / "store")
    with pytest.raises(FileNotFoundError):
        mod.upload_seed(specs_root=specs_root, bot="unified", store=store)


def test_ethics_seed_key_matches_reader_key(tmp_path: Path):
    """Upload key == reader key for ethics_decisions (FLAG 1 confirmation)."""
    mod = _load_script()
    specs_root = _make_specs(tmp_path / "specs")
    store = LocalFsStore(tmp_path / "store")
    mod.upload_seed(specs_root=specs_root, bot="unified", store=store)

    # The ethics fetcher reads from: seed/unified/ethics_decisions/index.csv
    # (config.seed_key when wired by fetch_and_process.py)
    key = "seed/unified/ethics_decisions/index.csv"
    assert store.exists(key)
    content = store.get_bytes(key).decode("utf-8")
    assert "url,filename,date,knesset_num,title" in content


def test_lexicon_overrides_key_matches_reader_key(tmp_path: Path):
    """Upload key == reader key for lexicon_section_overrides (FLAG 2 confirmation)."""
    mod = _load_script()
    specs_root = _make_specs(tmp_path / "specs")
    store = LocalFsStore(tmp_path / "store")
    mod.upload_seed(specs_root=specs_root, bot="unified", store=store)

    # The lexicon reader reads from: seed/unified/lexicon_section_overrides.json
    # (store.get_bytes(f'seed/{bot}/{_OVERRIDES_FILENAME}'))
    key = "seed/unified/lexicon_section_overrides.json"
    assert store.exists(key)
    import json
    data = json.loads(store.get_bytes(key))
    assert isinstance(data, dict)


def test_common_knowledge_key_matches_reader_key(tmp_path: Path):
    """Upload key == reader key for common-knowledge.md (FLAG 3 confirmation)."""
    mod = _load_script()
    specs_root = _make_specs(tmp_path / "specs")
    store = LocalFsStore(tmp_path / "store")
    mod.upload_seed(specs_root=specs_root, bot="unified", store=store)

    # The collect_sources reader reads from: seed/unified/common-knowledge.md
    # (store.get_bytes(f'seed/{bot}/{source}') where source='common-knowledge.md')
    key = "seed/unified/common-knowledge.md"
    assert store.exists(key)
    content = store.get_bytes(key).decode("utf-8")
    assert "part A" in content
