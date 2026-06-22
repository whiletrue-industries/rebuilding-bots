#!/usr/bin/env python3
"""Backfill a normalized `law_name` field into israeli_laws document metadata.

`law_name` = the base law title (DocumentTitle's prefix before " - "), which
collapses the section/chapter title variants ("תקנון הכנסת", "תקנון הכנסת - חלק
ד׳…", "תקנון הכנסת - הליך החקיקה" → all "תקנון הכנסת"). This is what the
israeli_laws retrieve op's `metadata_filter={"law_name":"<law>"}` matches via
JSONB exact containment, scoping a search to ONE law and eliminating cross-law
section-number collisions (hundreds of laws share a "סעיף 86").

Metadata-only UPDATE — does not touch embeddings or content_hash. Run after an
israeli_laws sync (the gap laws / rich-metadata docs gain DocumentTitle; the
embed-only thin docs have none and are skipped). Reads DATABASE_URL from env;
defaults to the local aurora-local container.

    DATABASE_URL=postgresql://botnim:botnim@localhost:54320/botnim_local \\
        python scripts/backfill-law-name.py
"""
import os
import psycopg

DSN = os.environ.get("DATABASE_URL", "postgresql://botnim:botnim@localhost:54320/botnim_local")
# session.py-style normalisation: psycopg connects with a plain libpq DSN
if DSN.startswith("postgresql+psycopg://"):
    DSN = "postgresql://" + DSN[len("postgresql+psycopg://"):]

conn = psycopg.connect(DSN, autocommit=True)
n = conn.execute("""
    UPDATE documents d
    SET metadata = d.metadata || jsonb_build_object(
        'law_name', trim(split_part(d.metadata->>'DocumentTitle', ' - ', 1)))
    FROM contexts c
    WHERE d.context_id = c.id AND c.name = 'israeli_laws'
      AND d.metadata ? 'DocumentTitle'
      AND coalesce(d.metadata->>'DocumentTitle', '') <> ''
""").rowcount
print(f"backfilled law_name on {n} israeli_laws documents")
