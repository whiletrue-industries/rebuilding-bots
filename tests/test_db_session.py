import os

import pytest
from sqlalchemy import text

from botnim.db.session import get_engine, get_session


def test_get_engine_reads_database_url(monkeypatch):
    monkeypatch.delenv("DB_HOST", raising=False)
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@h:5432/d")
    # Force re-init
    from botnim.db import session as s
    s._engine = None
    eng = get_engine()
    assert "postgresql" in str(eng.url)
    assert eng.url.host == "h"


def test_get_engine_assembles_from_per_field_vars(monkeypatch):
    """Per-field env vars (the shared-ecs-app preferred pattern) take precedence
    over DATABASE_URL when both are set."""
    monkeypatch.setenv("DB_HOST", "aurora.example.com")
    monkeypatch.setenv("DB_PORT", "5432")
    monkeypatch.setenv("DB_NAME", "botnim_staging")
    monkeypatch.setenv("DB_USER", "botnim_app")
    monkeypatch.setenv("DB_PASSWORD", "p@ss/word")  # special chars verify URL-quoting
    monkeypatch.setenv("DATABASE_URL", "postgresql://ignored:ignored@ignored/ignored")
    from botnim.db import session as s
    s._engine = None
    eng = get_engine()
    assert eng.url.host == "aurora.example.com"
    assert eng.url.username == "botnim_app"
    assert eng.url.database == "botnim_staging"
    # password is URL-decoded by SQLAlchemy when accessed via .url.password
    assert eng.url.password == "p@ss/word"


def test_get_engine_raises_when_no_url(monkeypatch):
    monkeypatch.delenv("DB_HOST", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.delenv("BOTNIM_DATABASE_URL", raising=False)
    from botnim.db import session as s
    s._engine = None
    with pytest.raises(RuntimeError, match="DB_HOST.*DATABASE_URL"):
        get_engine()


def test_get_engine_caches(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@h:5432/d")
    e1 = get_engine()
    e2 = get_engine()
    assert e1 is e2


def test_session_executes_against_real_pg(database_url, monkeypatch):
    """Real round-trip — proves the session actually connects."""
    monkeypatch.setenv("DATABASE_URL", database_url)
    # Force engine cache reset
    from botnim.db import session as s
    s._engine = None
    with get_session() as sess:
        result = sess.execute(text("SELECT 1")).scalar()
    assert result == 1
