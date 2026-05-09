"""Wrapper that imports the renderer's pure function.

Keeps the package boundary clean: callers import from botnim.sanity.render,
not from a sibling scripts/ path.
"""
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

# scripts/ is COPYed into /srv/scripts in the Dockerfile; locally it's a
# sibling of botnim/ in the rebuilding-bots checkout.
_SCRIPTS_DIR = Path(__file__).resolve().parent.parent.parent / "scripts"
_RENDERER_PATH = _SCRIPTS_DIR / "render-sanity-html.py"

# Load the module from its file path (the hyphen in the filename prevents
# using importlib.import_module by name directly).
_spec = importlib.util.spec_from_file_location("render_sanity_html", _RENDERER_PATH)
_renderer = importlib.util.module_from_spec(_spec)
sys.modules["render_sanity_html"] = _renderer
_spec.loader.exec_module(_renderer)


def render_html(capture, judged, *, title: str = "Sanity DoD") -> str:
    return _renderer.render(capture, judged, title=title)
