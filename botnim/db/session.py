"""Database session factory.

Reads DB_HOST / DB_PORT / DB_NAME / DB_USER / DB_PASSWORD from the
process environment (the canonical pattern for shared-ecs-app
consumers — see modules/shared-ecs-app/README.md). Falls back to
DATABASE_URL for local dev. Engine is cached at module load time
(one engine per process — same pattern as the existing ES client).
"""
from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Iterator
from urllib.parse import quote_plus

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

_engine: Engine | None = None
_SessionFactory: sessionmaker | None = None


def _build_database_url() -> str:
    # Preferred path: per-field env vars (set by ECS task def via
    # shared-ecs-app secret_environment_variables JSON-key selectors).
    host = os.getenv("DB_HOST")
    if host:
        port = os.getenv("DB_PORT", "5432")
        dbname = os.getenv("DB_NAME") or "botnim_staging"
        user = os.getenv("DB_USER") or "botnim_app"
        password = os.getenv("DB_PASSWORD", "")
        return f"postgresql+psycopg://{quote_plus(user)}:{quote_plus(password)}@{host}:{port}/{dbname}"
    # Fallback: explicit DATABASE_URL (local dev / pytest-postgresql / cutover scripts)
    db_url = os.getenv("DATABASE_URL") or os.getenv("BOTNIM_DATABASE_URL")
    if not db_url:
        raise RuntimeError(
            "DB_HOST + DB_PORT/DB_NAME/DB_USER/DB_PASSWORD or DATABASE_URL "
            "must be set. Aurora backend requires Postgres connection info."
        )
    # Normalise bare postgresql:// → postgresql+psycopg:// so the v3 driver is
    # selected (psycopg2 is not installed in this project).
    if db_url.startswith("postgresql://"):
        db_url = "postgresql+psycopg://" + db_url[len("postgresql://"):]
    return db_url


def get_engine() -> Engine:
    global _engine, _SessionFactory
    if _engine is not None:
        return _engine
    _engine = create_engine(_build_database_url(), pool_pre_ping=True)
    _SessionFactory = sessionmaker(bind=_engine, expire_on_commit=False)
    return _engine


@contextmanager
def get_session() -> Iterator[Session]:
    """Yield a SQLAlchemy session and commit on success / rollback on error."""
    get_engine()  # ensures both _engine and _SessionFactory are initialised
    assert _SessionFactory is not None
    sess = _SessionFactory()
    try:
        yield sess
        sess.commit()
    except Exception:
        sess.rollback()
        raise
    finally:
        sess.close()
