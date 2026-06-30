"""Ensure per-context partial HNSW indexes on documents.embedding.

WHY: pgvector's HNSW index cannot filter by `context_id` during graph traversal.
A vector search on the shared `documents` table (all contexts under one global
HNSW index) therefore applies `context_id` as a POST-filter and over-traverses
the global graph to collect enough matches for one context — slow on large
contexts. A *partial* HNSW index `WHERE context_id = <ctx>` makes the search
traverse only that context's vectors (no post-filter). Verified on staging
2026-06-29: israeli_laws vector search dropped from a global-graph scan to ~1.7s.

This is the durable, reproducible companion to the manual index built on staging.
Run it on any env (staging/prod) and after any DB rebuild. Idempotent — it skips
indexes that already exist.

USAGE (inside the botnim-api task, e.g. via `aws ecs run-task` overrides or
ECS Exec — see parlibot/.claude/skills/botnim-ad-hoc-task):
    python scripts/ensure-partial-hnsw-indexes.py

Env overrides: HNSW_M (default 32), HNSW_EF_CONSTRUCTION (256),
INDEX_MAINTENANCE_WORK_MEM (1GB).
"""
import os
from botnim.db.session import get_engine
from sqlalchemy import text

# (bot, context_name, index_name). Add large/hot contexts whose filtered vector
# searches are slow. israeli_laws (~185K docs) is the confirmed offender.
TARGETS = [
    ("unified", "israeli_laws", "documents_embedding_hnsw_il"),
]
M = int(os.environ.get("HNSW_M", "32"))
EFC = int(os.environ.get("HNSW_EF_CONSTRUCTION", "256"))
MWM = os.environ.get("INDEX_MAINTENANCE_WORK_MEM", "1GB")


def main() -> None:
    eng = get_engine()
    with eng.connect() as conn:
        c = conn.execution_options(isolation_level="AUTOCOMMIT")  # CONCURRENTLY needs no txn
        try:
            c.execute(text(f"SET maintenance_work_mem='{MWM}'"))
        except Exception as e:  # noqa: BLE001 - non-fatal tuning
            print(f"warn: could not set maintenance_work_mem: {str(e)[:120]}", flush=True)
        for bot, name, idx in TARGETS:
            cid = c.execute(
                text("SELECT id FROM contexts WHERE bot=:b AND name=:n"),
                {"b": bot, "n": name},
            ).scalar()
            if not cid:
                print(f"[skip] context {bot}/{name} not found — sync it first", flush=True)
                continue
            exists = c.execute(
                text("SELECT 1 FROM pg_class WHERE relname=:i AND relkind='i'"),
                {"i": idx},
            ).scalar()
            if exists:
                print(f"[ok] {idx} already exists for {bot}/{name}", flush=True)
                continue
            print(f"[build] {idx} for {bot}/{name} (context_id={cid}) — this takes minutes...", flush=True)
            c.execute(text(
                f"CREATE INDEX CONCURRENTLY {idx} ON documents USING hnsw "
                f"(embedding vector_cosine_ops) WITH (m={M}, ef_construction={EFC}) "
                f"WHERE context_id = '{cid}'"
            ))
            valid = c.execute(text(
                "SELECT indisvalid FROM pg_index WHERE indexrelid=:i::regclass"), {"i": idx}).scalar()
            print(f"[done] {idx} created (valid={valid})", flush=True)


if __name__ == "__main__":
    main()
