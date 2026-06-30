"""Aurora (PostgreSQL + pgvector) vector store backend.

Mirrors the surface of VectorStoreES so sync.py can swap backends
via the --backend flag. See docs/superpowers/specs/2026-04-26-aurora-migration-design.md
for design rationale.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
from datetime import datetime
from typing import Any

import tiktoken
from openai import OpenAI
from sqlalchemy import bindparam, text

from ..config import is_production, get_logger, DEFAULT_EMBEDDING_SIZE, DEFAULT_EMBEDDING_MODEL
from ..db.session import get_engine, get_session
from .vector_store_base import VectorStoreBase

logger = get_logger(__name__)


# Code-level defaults for retrieval-shape knobs. Per-context overrides live
# in `specs/<bot>/config.yaml` next to each context entry — see
# `_CHUNKING_CONFIG_KEYS` and `_HNSW_EF_SEARCH_KEY` below for the exact YAML
# fields. We don't expose env-var overrides for these any more — they're
# corpus-shape decisions, so they belong in the data-definition file alongside
# `sources:` / `max_num_results:`, not in deployment env vars.
#
# Defaults:
#   chunk_max_tokens     = 600   (small chunks → tighter embedding centroids,
#                                 dramatically better recall on Hebrew long docs;
#                                 see retrieval-strategy A/B 2026-05-10).
#   chunk_overlap_tokens = 80    (~13% of chunk size, standard RAG ratio.)
#   hnsw_ef_search       = 100   (pgvector default 40 missed instructional docs
#                                 in favour of one-line metadata-style entries
#                                 that share more surface tokens with queries.
#                                 100 trades a small latency hit for recall.
#                                 The historical 500 was over-fitted to a
#                                 small offline benchmark and contributed to
#                                 the 2026-05-09 prod 504s.)
#
# `government_decisions` pins chunk_max_tokens/overlap to 600/80 in config.yaml
# — matching the code default. (Caveat: aurora_writer.py's write_decision /
# write_decisions_batched call _chunk_for_embedding without forwarding the
# per-context override, so the YAML values for this context are documentary
# rather than load-bearing right now; fixing that requires plumbing the
# context dict into those call sites.)
_CHUNK_MAX_TOKENS_DEFAULT = 600
_CHUNK_OVERLAP_TOKENS_DEFAULT = 80
_HNSW_EF_SEARCH_DEFAULT = 100
_HNSW_EF_SEARCH_MIN = 10
_HNSW_EF_SEARCH_MAX = 1000

# Per-context lexical strategies. `tsquery` is the existing prefix-OR
# BM25 path; `trigram` uses pg_trgm.word_similarity() against the
# documents_content_trgm GIN index (added in alembic 0015).
#
# Trigram is the Hebrew-friendly option: 3-gram character matching
# bridges construct-state alternation (ועדת↔ועדה↔ועדות) without
# needing a real Hebrew tsconfig. Local A/B on the prod query
# "מה הדרך ליזום ועדת חקירה ממלכתית?" — trigram surfaced all 3
# prod-cited sections in top-8; tsquery surfaced 0/3.
_LEXICAL_STRATEGY_TSQUERY = "tsquery"
_LEXICAL_STRATEGY_TRIGRAM = "trigram"
_LEXICAL_STRATEGIES = frozenset({_LEXICAL_STRATEGY_TSQUERY, _LEXICAL_STRATEGY_TRIGRAM})

# pg_trgm.word_similarity_threshold default is 0.6 — too strict for our
# corpus (real-relevance hits scored 0.55-0.76 in 2026-05-13 probes,
# so the default cuts off the top of the ranking). 0.1 is a wide net
# that still beats unrelated docs by a healthy margin under RRF.
# Set via SET LOCAL inside the search txn — never leaks.
_TRIGRAM_WORD_SIMILARITY_THRESHOLD = 0.1

# Punctuation-only law_name normalization. Applied identically to the query-side
# filter value (Python) and the stored value (SQL, _LAW_NAME_NORM_SQL) so a model
# that emits "חוק-יסוד: הממשלה" matches a stored "חוק-יסוד הממשלה" / "חוק־יסוד: ...".
_LAW_NAME_NORM_TRANSFORMS = [("־", "-"), (":", " "), ("״", '"'), ("׳", "'")]


def _normalize_law_name(value):
    if value is None:
        return None
    s = str(value)
    for src, dst in _LAW_NAME_NORM_TRANSFORMS:
        s = s.replace(src, dst)
    return re.sub(r"\s+", " ", s).strip()


# Same transform as _normalize_law_name, in SQL, over metadata->>'law_name'.
# '׳'→'''' is the SQL escaped apostrophe; '\\s+' is the Python-escaped regex \s+.
_LAW_NAME_NORM_SQL = (
    "trim(regexp_replace("
    "replace(replace(replace(replace(metadata->>'law_name', '־', '-'), ':', ' '), '״', '\"'), '׳', ''''),"
    " '\\s+', ' ', 'g'))"
)

def _build_metadata_filter_sql(metadata_filter):
    """Build the WHERE fragment + params for a metadata_filter.

    `law_name` is matched by NORMALIZED equality (punctuation-insensitive) so the
    model's "חוק-יסוד: הממשלה" hits the stored "חוק-יסוד הממשלה". Any other keys keep
    the original JSONB containment (`@>`).
    """
    if not metadata_filter:
        return "", {}
    clauses = []
    params = {}
    rest = dict(metadata_filter)
    law = rest.pop("law_name", None)
    if law is not None:
        clauses.append(f" AND {_LAW_NAME_NORM_SQL} = :law_norm")
        params["law_norm"] = _normalize_law_name(str(law))
    if rest:
        clauses.append(" AND metadata @> CAST(:mfilter AS jsonb)")
        params["mfilter"] = json.dumps(rest)
    return "".join(clauses), params


_LAW_NAME_RESOLVE_THRESHOLD = 0.45


def _best_law_match(sess, cid, mention, threshold=_LAW_NAME_RESOLVE_THRESHOLD):
    """Best-matching formal law_name + its trigram similarity over the distinct
    law_name set (the `law_name_catalog` matview, ~14k rows vs ~185k docs → ~200ms),
    or None. The `%` operator is gated by pg_trgm.similarity_threshold; we set it to
    `threshold`. `law_name_catalog_trgm` serves the `%` lookup. Returns (law_name, score).
    """
    if not mention:
        return None
    sess.execute(text("SET LOCAL pg_trgm.similarity_threshold = %s" % float(threshold)))
    row = sess.execute(text(
        "SELECT law_name, similarity(law_name, :m) AS s "
        "FROM law_name_catalog "
        "WHERE context_id = :cid AND law_name % :m "
        "ORDER BY s DESC LIMIT 1"
    ), {"cid": cid, "m": mention}).fetchone()
    if row and row[1] is not None:
        return (row[0], float(row[1]))
    return None


def _resolve_law_name(sess, cid, mention, threshold=_LAW_NAME_RESOLVE_THRESHOLD):
    """Resolve a colloquial/partial/variant law mention to the formal `law_name`
    in this context (pg_trgm similarity over law_name_catalog), or None if nothing
    is similar enough. Used on a scoped-filter exact-miss to rescue the scope
    (e.g. the model's "חוק המכרזים" -> "חוק חובת המכרזים").
    """
    m = _best_law_match(sess, cid, mention, threshold)
    if m is not None and m[1] >= threshold:
        return m[0]
    return None


_QUERY_DETECT_THRESHOLD = 0.55
_QUERY_DETECT_DOMINANCE = 0.5  # the resolved law-name span must cover at least this fraction of the
                                # query's content tokens; below it the prefix is incidental -> abstain

# Legal-title prefix tokens (an optional single leading letter — ה article, ל/ב/כ prepositions — is stripped before matching).
_LEGAL_PREFIXES = frozenset({
    "חוק", "חוק-יסוד", "תקנון", "תקנות", "פקודת", "פקודה", "כללי", "צו", "הוראות",
})
# Tokens that end a law-name span (question words, conjunctions).
_SPAN_BOUNDARY = frozenset({
    "מה", "מהו", "מהי", "האם", "מתי", "איך", "כמה", "מי", "למה", "איזה", "ו", "או", "אבל",
})
_DETECT_PUNCT = "?,.!׳״()\"':;"


def _detect_law_in_query(sess, cid, query_text, threshold=_QUERY_DETECT_THRESHOLD):
    """If an unfiltered israeli_laws query names a specific law, return the formal
    law_name (resolved via _best_law_match over law_name_catalog), else None.

    Gate: the query must contain EXACTLY ONE legal-prefix token (חוק/תקנון/...);
    zero or more-than-one (ambiguous) -> None. From that prefix, try spans of
    prefix + 1..3 following content tokens (stopping at a boundary word / punctuation),
    requiring >=1 content token; resolve each and keep the single best match >= threshold.
    """
    if not query_text:
        return None
    raw = query_text.split()
    toks = [t.strip(_DETECT_PUNCT) for t in raw]
    had_punct = [t != t.strip(_DETECT_PUNCT) for t in raw]  # token carried a trailing/leading boundary mark

    def _is_prefix(tok):
        # strip a single leading letter (ה=article; ל/ב/כ=prepositions) if the remainder is a legal prefix
        t = tok[1:] if len(tok) > 1 and tok[1:] in _LEGAL_PREFIXES else tok
        return t in _LEGAL_PREFIXES

    prefix_idxs = [i for i, t in enumerate(toks) if t and _is_prefix(t)]
    if len(prefix_idxs) != 1:
        return None
    i = prefix_idxs[0]

    best = None  # (law_name, score, span_len)
    for k in (1, 2, 3):
        end = i + k
        if end >= len(toks):
            break
        nxt = toks[end]
        if not nxt or nxt in _SPAN_BOUNDARY:
            break
        span = " ".join(toks[i:end + 1])
        m = _best_law_match(sess, cid, span, threshold)
        if m is not None and m[1] >= threshold and (best is None or m[1] > best[1]):
            best = (m[0], m[1], end - i + 1)
        if had_punct[end]:  # this token ended a sentence clause — stop extending
            break
    if best is None:
        return None
    # Dominance gate: an incidental legal-prefix span (e.g. a trailing "תקנון הכנסת" while the
    # query's subject is elsewhere) must NOT hijack the scope. Require the span to cover a
    # dominant share of the query's content tokens; else abstain -> the unfiltered topical search.
    content_len = sum(1 for t in toks if t and t not in _SPAN_BOUNDARY)
    if content_len and best[2] / content_len < _QUERY_DETECT_DOMINANCE:
        return None
    return best[0]


_SCOPED_OVERRIDE_VECTOR_WEIGHT = 0.5


def _scoped_vector_knn_sql(rest_sql):
    """SQL for an EXACT vector KNN over a single law's docs.

    `AS MATERIALIZED` is MANDATORY: Postgres 12+ inlines a single-reference CTE
    by default, which would let the planner merge the law_name filter with the
    `ORDER BY embedding <=>` and plan a GLOBAL HNSW scan + post-filter — that can
    return 0 for a selective law even though its docs exist. Materializing the
    law's docs first makes the outer ORDER BY ... LIMIT an exact KNN over that
    small set. `rest_sql` carries any non-law_name filter keys so the scoped set
    matches the FULL filter.
    """
    return (
        "WITH law_docs AS MATERIALIZED ("
        " SELECT id, content, metadata, embedding FROM documents"
        # `metadata ? 'law_name'` is REQUIRED for the planner to use the partial
        # index documents_law_name_norm (migration 0016) -> O(law) filter. It is
        # semantically redundant with the norm-equality clause but the planner
        # cannot infer the implication through the replace/regexp chain.
        f" WHERE context_id = :cid AND metadata ? 'law_name' AND {_LAW_NAME_NORM_SQL} = :law_norm{rest_sql}"
        ")"
        " SELECT id, content, metadata, 1 - (embedding <=> CAST(:emb AS vector)) AS score"
        " FROM law_docs ORDER BY embedding <=> CAST(:emb AS vector) LIMIT :limit"
    )


def _scoped_vector_knn(sess, cid, law_norm, rest_sql, rest_params, embedding, fetch):
    """Exact vector KNN over one law's docs (see _scoped_vector_knn_sql). `sess` is
    the caller's transaction; hnsw.ef_search is irrelevant here (no HNSW on the
    materialized rows)."""
    return sess.execute(text(_scoped_vector_knn_sql(rest_sql)), {
        "cid": cid, "law_norm": law_norm, "emb": str(embedding), "limit": fetch, **rest_params,
    }).fetchall()


# Back-compat aliases for callers that import the old names.
CHUNK_MAX_TOKENS = _CHUNK_MAX_TOKENS_DEFAULT
CHUNK_OVERLAP_TOKENS = _CHUNK_OVERLAP_TOKENS_DEFAULT
EMBEDDING_TOKEN_LIMIT = 8192  # OpenAI text-embedding-3-* hard ceiling


def _resolve_int_setting(
    context: dict | None,
    key: str,
    default: int,
    *,
    minimum: int = 1,
    maximum: int | None = None,
) -> int:
    """Read an integer setting from a per-context config dict, with fallback
    to the code-level default. Logs a warning + clamps if the YAML value is
    out of range so a typo doesn't quietly break production search.

    `context` is one entry from `config['context']` (the parsed YAML), or
    None when the caller doesn't have that handy (during e.g. embedding
    fan-out for a doc that hasn't been associated with a context yet).
    """
    if not context:
        return default
    raw = context.get(key)
    if raw is None:
        return default
    try:
        n = int(raw)
    except (TypeError, ValueError):
        logger.warning(
            "context %r has non-int %s=%r; falling back to %d",
            context.get('slug'), key, raw, default,
        )
        return default
    if n < minimum or (maximum is not None and n > maximum):
        bound_max = maximum if maximum is not None else 'inf'
        logger.warning(
            "context %r has %s=%d outside [%d, %s]; clamping",
            context.get('slug'), key, n, minimum, bound_max,
        )
        n = max(minimum, n if maximum is None else min(maximum, n))
    return n


_tokenizer = None


def _get_tokenizer():
    global _tokenizer
    if _tokenizer is None:
        # cl100k_base matches text-embedding-3-small / -large.
        _tokenizer = tiktoken.encoding_for_model(DEFAULT_EMBEDDING_MODEL)
    return _tokenizer


def _chunk_for_embedding(
    content: str,
    max_tokens: int = CHUNK_MAX_TOKENS,
    overlap_tokens: int = CHUNK_OVERLAP_TOKENS,
) -> list[str]:
    """Split content into chunks each safely under the embedding token limit.

    Splitting strategy, in priority order:
        1. Markdown section boundaries (lines beginning with `##`)
        2. Paragraph boundaries (blank lines)
        3. Sentence boundaries (period+space, naive — Hebrew/English heuristic)
        4. Hard token-count split (last resort, mid-sentence)

    Each chunk includes ~`overlap_tokens` of the previous chunk's tail to
    avoid losing context at split points (a query landing right at a chunk
    boundary still hits at least one chunk that contains the surrounding
    context). Standard RAG technique — see e.g. LangChain's
    RecursiveCharacterTextSplitter for the same idea.

    Content shorter than `max_tokens` returns as a single-element list — no
    splitting work, no overlap. The vast majority of files take this path
    and pay zero tokenization cost beyond the initial count.
    """
    enc = _get_tokenizer()
    token_ids = enc.encode(content)
    if len(token_ids) <= max_tokens:
        return [content]

    # Multi-chunk path: split at semantic boundaries first.
    # Try ## section headers — split keeping the marker with the following text.
    sections = re.split(r"(?m)(?=^##\s)", content)
    sections = [s for s in sections if s.strip()]
    if len(sections) <= 1:
        # Fallback: split on blank-line paragraph boundaries.
        sections = re.split(r"\n\s*\n", content)
        sections = [s for s in sections if s.strip()]

    # Greedy pack sections into chunks under the limit; if a single section
    # is itself too large, hard-split it by token windows with overlap.
    chunks: list[str] = []
    current: list[str] = []
    current_tokens = 0
    for section in sections:
        section_tokens = len(enc.encode(section))
        if section_tokens > max_tokens:
            # Section itself oversized — flush whatever we've packed, then
            # hard-split this section by token windows.
            if current:
                chunks.append("\n\n".join(current))
                current = []
                current_tokens = 0
            section_token_ids = enc.encode(section)
            step = max_tokens - overlap_tokens
            for start in range(0, len(section_token_ids), step):
                window = section_token_ids[start : start + max_tokens]
                chunks.append(enc.decode(window))
                if start + max_tokens >= len(section_token_ids):
                    break
            continue
        if current_tokens + section_tokens > max_tokens and current:
            chunks.append("\n\n".join(current))
            # Carry overlap from end of previous chunk into the new one.
            prev_tail_tokens = enc.encode(chunks[-1])[-overlap_tokens:]
            tail_text = enc.decode(prev_tail_tokens) if prev_tail_tokens else ""
            current = [tail_text, section] if tail_text else [section]
            current_tokens = (
                len(prev_tail_tokens) + section_tokens
            ) if tail_text else section_tokens
        else:
            current.append(section)
            current_tokens += section_tokens
    if current:
        chunks.append("\n\n".join(current))

    return chunks


def _get_embedding_client(environment: str):
    """Return an object with an .embed(text) -> list[float] method.

    Real impl returns a thin wrapper around OpenAI; tests monkeypatch
    this function to inject a fake. Kept as a module-level function
    (not a method) so monkeypatch works without subclassing.
    """
    # Centralised through _resolve_openai_api_key so the contextvar in
    # botnim.config picks up the OPENAI_API_KEY_<ENV>_FAP_SYNC override
    # when this client is built inside a fap_sync_context().
    from botnim.config import _resolve_openai_api_key
    client = OpenAI(api_key=_resolve_openai_api_key(environment))

    class _Wrapper:
        def embed(self, text: str) -> list:
            response = client.embeddings.create(
                input=text,
                model=DEFAULT_EMBEDDING_MODEL,
            )
            return response.data[0].embedding

    return _Wrapper()


class VectorStoreAurora(VectorStoreBase):
    """Vector store backed by Aurora Serverless v2 (PostgreSQL 16.4 + pgvector)."""

    def __init__(self, config: dict, config_dir, environment: str | None = None):
        if environment is None:
            raise ValueError(
                "Environment must be explicitly specified. "
                "Use 'local', 'staging', or 'production'"
            )
        env_name = environment.lower()
        if env_name not in {"local", "staging", "production"}:
            raise ValueError(
                f"Invalid environment: {environment}. "
                "Must be one of: local, staging, production"
            )
        production = is_production(env_name)
        super().__init__(config, config_dir, production=production)
        self.environment = env_name

        # OpenAI client surface — query.py expects `vector_store.openai_client.embeddings.create(...)`
        # to mint query embeddings. Centralised through _resolve_openai_api_key
        # so the contextvar in botnim.config picks up the
        # OPENAI_API_KEY_<ENV>_FAP_SYNC override when this store is built
        # inside a fap_sync_context() (i.e. during the daily refresh, not
        # at chat-retrieval time).
        from botnim.config import _resolve_openai_api_key
        self.openai_client = OpenAI(api_key=_resolve_openai_api_key(self.environment))

        # Trigger engine creation early so connection failures surface here, not later
        get_engine()
        logger.info("VectorStoreAurora initialized for environment=%s", env_name)

    def _supports_extraction_cache(self) -> bool:
        """Aurora backend talks directly to the same DB that hosts the
        extraction_cache table — always supported."""
        return True

    # ---- abstract method overrides -----------------------------------------

    def get_or_create_vector_store(self, context, context_name, replace_context, force_rebuild=False):
        """Return the context_id (uuid str) for (bot, context_name).

        - Inserts a row into contexts if it doesn't exist.
        - If force_rebuild is True, deletes all rows in documents that
          reference this context (CASCADE handles the join). Default
          behavior keeps existing rows so upload_files's content-hash skip
          can reuse them — that's the delta-sync path.

        replace_context is preserved in the signature for caller-API
        compatibility but no longer drives the DELETE; force_rebuild does.
        """
        bot = self.config["slug"]
        with get_session() as sess:
            row = sess.execute(text(
                "INSERT INTO contexts (bot, name) VALUES (:bot, :name) "
                "ON CONFLICT (bot, name) DO UPDATE SET updated_at=now() "
                "RETURNING id"
            ), {"bot": bot, "name": context_name}).fetchone()
            cid = str(row[0])

            if force_rebuild:
                sess.execute(text(
                    "DELETE FROM documents WHERE context_id = :cid"
                ), {"cid": cid})
                logger.info("Cleared documents for context %s/%s (id=%s) — force_rebuild",
                            bot, context_name, cid)
        return cid

    def upload_files(self, context, context_name, vector_store, file_streams, callback):
        """Insert one row per chunk of each markdown file.

        Most files fit in a single chunk (and produce a single row, the
        backward-compatible shape). Files whose content exceeds the embedding
        model's token limit are split into multiple chunks at semantic
        boundaries (`_chunk_for_embedding`); each chunk gets its own row,
        its own embedding, its own content_hash. Retrieval naturally surfaces
        whichever chunk(s) match a query best — this both eliminates the
        oversize-doc skip behavior AND improves retrieval precision (chunked
        embeddings represent one concept tightly rather than averaging an
        entire long document).

        Each row's `metadata.filename` is the source file. For multi-chunk
        files, `metadata.chunk_index` and `metadata.total_chunks` are added so
        the LLM citation layer can collapse same-source chunks into one
        citation if it wants.

        Per-chunk errors (embedding failures, malformed UTF-8) are logged and
        skipped at the chunk level — one bad chunk does not abort the batch
        nor the rest of the file's chunks. Mirrors VectorStoreES's
        `return_exceptions=True` semantics for the file-level path.
        """
        cid = vector_store  # this is the context_id uuid (returned by get_or_create)
        client = _get_embedding_client(self.environment)
        successful = 0  # row count, not file count
        skipped = 0
        files_seen = 0
        # Per-chunk delta breakdown — SYNC_DELTA observability (2026-05-27).
        # Before, `successful` counted both "skipped because exists" and
        # "INSERTed because new" together, so log readers couldn't tell
        # whether a run actually surfaced new content or just paid LLM cost
        # to re-embed chunks whose content_hash drifted. These split the
        # counter so the per-context SYNC_DELTA line can expose
        # chunks_inserted (real work) vs chunks_unchanged (cache hits)
        # vs orphans_deleted (reconcile replacements).
        chunks_unchanged = 0  # (cid, content_hash) row already existed → no embed, no INSERT
        chunks_inserted  = 0  # row didn't exist → embed + INSERT happened
        # Every chunk content_hash this run produces (whether skipped as
        # unchanged or freshly inserted). Used after the upload pass to
        # delete rows whose content the run no longer produces.
        seen_hashes: set[str] = set()
        # Distinct filenames this run actually processed. The reconcile is
        # scoped to this set so rows from files the run did NOT read are
        # NEVER deleted — protects e.g. historical seeds whose source file
        # is not in the current run's input but whose chunks are still
        # legitimate. Without this scope the reconcile becomes a
        # whole-context wipe whenever the run's input is incomplete (e.g.
        # an EFS seed-stickiness window leaving an old subset on disk).
        files_processed: set[str] = set()

        # Per-context chunking — see _CHUNK_*_DEFAULT for rationale.
        # `context` here is the parsed YAML entry for this slug, so it carries
        # the optional `chunk_max_tokens` / `chunk_overlap_tokens` overrides.
        chunk_max = _resolve_int_setting(
            context, 'chunk_max_tokens', _CHUNK_MAX_TOKENS_DEFAULT,
            minimum=64, maximum=EMBEDDING_TOKEN_LIMIT,
        )
        chunk_overlap = _resolve_int_setting(
            context, 'chunk_overlap_tokens', _CHUNK_OVERLAP_TOKENS_DEFAULT,
            minimum=0, maximum=chunk_max // 2,
        )
        logger.info(
            "Chunking %s/%s with max_tokens=%d, overlap=%d",
            self.config.get('slug', '?'), context_name, chunk_max, chunk_overlap,
        )

        with get_session() as sess:
            for fname, content_file, file_type, metadata in file_streams:
                if not fname.endswith(".md"):
                    logger.debug("Skipping non-markdown file: %s", fname)
                    continue
                files_seen += 1
                files_processed.add(fname)

                try:
                    raw_content = content_file.read().decode("utf-8")
                except Exception as exc:
                    logger.error("Failed to read file %s: %s", fname, exc)
                    skipped += 1
                    continue

                chunks = _chunk_for_embedding(raw_content, max_tokens=chunk_max, overlap_tokens=chunk_overlap)
                total_chunks = len(chunks)
                if total_chunks > 1:
                    logger.info(
                        "Chunked %s into %d pieces (oversize content)",
                        fname, total_chunks,
                    )

                for chunk_index, chunk_content in enumerate(chunks):
                    try:
                        chunk_hash = hashlib.sha256(chunk_content.encode("utf-8")).hexdigest()
                        seen_hashes.add(chunk_hash)

                        # Content-hash skip: if this exact (context_id, content_hash)
                        # is already present, do nothing.
                        existing = sess.execute(text(
                            "SELECT id FROM documents "
                            "WHERE context_id=:cid AND content_hash=:h"
                        ), {"cid": cid, "h": chunk_hash}).fetchone()
                        if existing:
                            logger.debug(
                                "Skipping unchanged content for %s (chunk %d/%d)",
                                fname, chunk_index + 1, total_chunks,
                            )
                            successful += 1
                            chunks_unchanged += 1
                            continue

                        # New or changed content — embed and insert
                        embedding = client.embed(chunk_content)
                        doc_metadata = dict(metadata or {})
                        doc_metadata["filename"] = fname
                        doc_metadata["context_name"] = context_name
                        doc_metadata["context_type"] = context.get("type", "")
                        doc_metadata["extracted_at"] = datetime.utcnow().isoformat()
                        if total_chunks > 1:
                            doc_metadata["chunk_index"] = chunk_index
                            doc_metadata["total_chunks"] = total_chunks

                        sess.execute(text(
                            "INSERT INTO documents "
                            "(context_id, content, content_hash, metadata, embedding, source_id) "
                            "VALUES (:cid, :c, :h, CAST(:m AS jsonb), CAST(:e AS vector), :sid)"
                        ), {
                            "cid": cid,
                            "c": chunk_content,
                            "h": chunk_hash,
                            "m": json.dumps(doc_metadata),
                            "e": str(embedding),
                            "sid": (metadata or {}).get("source_id"),
                        })
                        successful += 1
                        chunks_inserted += 1
                    except Exception as exc:
                        logger.error(
                            "Failed to process %s (chunk %d/%d): %s",
                            fname, chunk_index + 1, total_chunks, exc,
                        )
                        skipped += 1
                        continue

            # Reconcile: delete stale chunks of files the current run
            # actually processed. Per-file scope (metadata.filename IN
            # files_processed) means rows from files NOT touched this run
            # are never deleted — a partial / incomplete input must never
            # wipe orthogonal context data. Per-hash filter (content_hash
            # NOT IN seen_hashes) cleans up chunks superseded by new
            # content within the files we did process.
            #
            # Safety guards:
            #   1. Empty seen_hashes — skip entirely (a transient zero-chunk
            #      run can never produce DELETEs).
            #   2. Empty files_processed — same; if no files were touched,
            #      no per-file delete is meaningful.
            if seen_hashes and files_processed:
                sess.execute(text(
                    "CREATE TEMP TABLE _seen_hashes (h text PRIMARY KEY) "
                    "ON COMMIT DROP"
                ))
                sess.execute(text(
                    "INSERT INTO _seen_hashes (h) "
                    "SELECT DISTINCT unnest(CAST(:hs AS text[]))"
                ), {"hs": list(seen_hashes)})
                sess.execute(text(
                    "CREATE TEMP TABLE _processed_files (f text PRIMARY KEY) "
                    "ON COMMIT DROP"
                ))
                sess.execute(text(
                    "INSERT INTO _processed_files (f) "
                    "SELECT DISTINCT unnest(CAST(:fs AS text[]))"
                ), {"fs": list(files_processed)})
                reconcile = sess.execute(text(
                    "DELETE FROM documents WHERE context_id = :cid "
                    "AND metadata->>'filename' IN (SELECT f FROM _processed_files) "
                    "AND content_hash NOT IN (SELECT h FROM _seen_hashes)"
                ), {"cid": cid})
                orphaned = reconcile.rowcount or 0
                logger.info(
                    "Reconcile: removed %d stale chunks across %d processed "
                    "files for context_id=%s (%d distinct content hashes kept)",
                    orphaned, len(files_processed), cid, len(seen_hashes),
                )
            else:
                orphaned = 0
                logger.warning(
                    "upload_files produced 0 chunks for context_id=%s — skipping "
                    "orphan reconcile so a transient empty run cannot wipe the "
                    "context.", cid,
                )

            # Structured per-context SYNC_DELTA marker (2026-05-27). One line,
            # grep-friendly. Distinguishes the three meaningful outcomes of a
            # delta sync that the unsplit `successful` counter could not:
            #   - chunks_unchanged: existed at the same content_hash → cache hit
            #   - chunks_inserted:  embedded + INSERTed (content_hash didn't exist)
            #   - orphans_deleted:  removed by per-file reconcile (stale chunks
            #                       in files the run touched but no longer
            #                       produces this content_hash for)
            #
            # churn_ratio = orphans_deleted / chunks_inserted:
            #   ~0.0 → inserts are net-new content (good, expected for genuine
            #          upstream additions)
            #   ~1.0 → every insert displaced an old chunk in the same file
            #          (= re-extraction churn — extraction non-determinism,
            #          chunking drift, etc. costs LLM money for ~0 new info)
            if chunks_inserted > 0:
                churn_pct_str = f"{int(round(100.0 * orphaned / chunks_inserted))}%"
            else:
                churn_pct_str = "N/A"
            logger.info(
                "SYNC_DELTA: bot=%s context=%s files_processed=%d "
                "chunks_unchanged=%d chunks_inserted=%d orphans_deleted=%d "
                "chunks_skipped_error=%d churn_ratio=%s",
                self.config.get('slug', '?'), context_name, len(files_processed),
                chunks_unchanged, chunks_inserted, orphaned,
                skipped, churn_pct_str,
            )

        if callable(callback):
            callback(successful)
        if skipped:
            logger.warning(
                "Uploaded %d rows from %d files to context_id=%s; skipped %d chunks with errors",
                successful, files_seen, cid, skipped,
            )
        else:
            logger.info(
                "Uploaded %d rows from %d files to context_id=%s",
                successful, files_seen, cid,
            )

    def delete_existing_files(self, context_, vector_store, file_names):
        """No-op — superseded by content-hash reconciliation in upload_files.

        The previous implementation ran a filename-based DELETE *before*
        upload_files. Because csv-type contexts name files positionally
        (``<context>_0.md`` … ``_N.md``), every run's file list matched
        every existing row, so this deleted the entire context — which in
        turn defeated upload_files' content-hash skip (the rows it would
        have reused were already gone), forcing a full re-embed of the
        whole corpus on every sync (~$0.55/day for knesset_protocols
        alone, even with no upstream changes).

        upload_files now keeps unchanged chunks via the content-hash skip
        and deletes only genuinely stale/orphan rows in a reconcile pass
        AFTER the upload. This method is kept because the base
        orchestrator calls it, but it intentionally does nothing.
        """
        return 0

    def _recency_search(
        self,
        context_name: str,
        num_results: int,
        metadata_filter: dict | None,
    ) -> dict:
        """Pure date-desc browse — returns the calendar-newest distinct
        documents in the context. Bypasses pgvector + tsvector entirely.

        Date field is resolved against a small list of known names because
        contexts disagree on which key holds the document date:
          - legal_advisor_letters / opinions: metadata->>'PublicationDate'
          - committee_decisions / ethics_decisions: metadata->>'תאריך'
          - older extraction schema: metadata->'extracted_data'->>'<same>'

        Dedup: each upstream document can be split into N chunks (chunk_index
        0..N-1, sharing `filename` / `source_id`). The query returns one row
        per dedup key (lowest chunk_index — usually 0 — picked deterministically
        via ROW_NUMBER). Without this the LLM saw 5 copies of the same letter.

        Filter: only documents whose date matches the ISO `YYYY-MM-DD` shape
        are eligible. Docs with a null or unparseable date are skipped (they
        can't be ranked by recency anyway). If the user wants those, REGULAR
        or METADATA_BROWSE is the right tool.
        """
        bot = self.config["slug"]
        with get_session() as sess:
            row = sess.execute(text(
                "SELECT id FROM contexts WHERE bot=:bot AND name=:name"
            ), {"bot": bot, "name": context_name}).fetchone()
            if not row:
                logger.warning(
                    "recency_search: context (%s, %s) not found", bot, context_name
                )
                return {"hits": {"hits": []}}
            cid = str(row[0])

            md_filter_sql, md_params = _build_metadata_filter_sql(metadata_filter)

            rows = sess.execute(text(f"""
                WITH dated AS (
                  SELECT
                    id, content, metadata,
                    COALESCE(
                      NULLIF(metadata->>'תאריך', ''),
                      NULLIF(metadata->>'תאריך_מכתב', ''),
                      NULLIF(metadata->>'PublicationDate', ''),
                      NULLIF(metadata->'extracted_data'->>'תאריך', ''),
                      NULLIF(metadata->'extracted_data'->>'תאריך_מכתב', ''),
                      NULLIF(metadata->'extracted_data'->>'PublicationDate', '')
                    ) AS doc_date,
                    COALESCE(
                      NULLIF(metadata->>'filename', ''),
                      NULLIF(metadata->>'source_id', ''),
                      id::text
                    ) AS dedup_key
                  FROM documents
                  WHERE context_id = :cid{md_filter_sql}
                ),
                ranked AS (
                  SELECT
                    id, content, metadata, doc_date,
                    ROW_NUMBER() OVER (
                      PARTITION BY dedup_key
                      ORDER BY COALESCE((metadata->>'chunk_index')::int, 0) ASC, id ASC
                    ) AS rn
                  FROM dated
                  WHERE doc_date IS NOT NULL
                    -- Strict: realistic year (1900-2039), valid month (01-12), valid day (01-31).
                    -- The loose `^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}` admitted garbage like
                    -- '3390-06-91' (committee_decisions _427.md), which sorted to position [0]
                    -- under ORDER BY doc_date DESC and poisoned every recency query.
                    AND doc_date ~ '^(19[0-9]{{2}}|20[0-3][0-9])-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])'
                )
                SELECT id, content, metadata
                FROM ranked
                WHERE rn = 1
                ORDER BY doc_date DESC, id DESC
                LIMIT :limit
            """), {"cid": cid, "limit": num_results, **md_params}).fetchall()

        hits = [{
            "_id": str(r[0]),
            "_score": 1.0,
            "_source": {"content": r[1], "metadata": r[2]},
        } for r in rows]
        return {"hits": {"hits": hits}}

    def search(
        self,
        context_name: str,
        query_text: str,
        search_mode,           # SearchModeConfig — kept for ES-parity signature
        embedding: list[float],
        num_results: int = 7,
        explain: bool = False,
        metadata_filter: dict | None = None,
        _qd_skip: bool = False,
    ) -> dict:
        """Hybrid retrieval: pgvector cosine + tsvector BM25, fused via
        reciprocal-rank-fusion. Mirrors VectorStoreES.search's return shape
        so downstream code (search_modes.py, the LLM tool layer) doesn't
        need to know which backend it talked to.

        Returns: {"hits": {"hits": [{"_id", "_score", "_source": {...}}, ...]}}
        """
        bot = self.config["slug"]
        # RECENCY_BROWSE bypasses similarity scoring entirely — pure date-desc
        # browse so the LLM can answer "latest X" correctly even when the query
        # has no strong topical signal. Output shape mirrors METADATA_BROWSE.
        if search_mode is not None and getattr(search_mode, "name", None) == "RECENCY_BROWSE":
            return self._recency_search(context_name, num_results, metadata_filter)
        fetch = num_results * 5  # over-fetch then RRF-trim
        # Honor the legacy ES SECTION_NUMBER / RELATED_RESOURCE contract: when
        # the mode declares `use_vector_search=False`, skip the pgvector branch
        # entirely so vectorally-similar-but-lexically-irrelevant docs don't
        # leak into results. vector_store_es.py:165 had this; the Aurora port
        # dropped it, which silently turned SECTION_NUMBER into REGULAR.
        use_vector = bool(getattr(search_mode, "use_vector_search", True))
        # Lexical (BM25 / tsvector) branch. Default lives on the search mode —
        # REGULAR + METADATA_BROWSE ship with it OFF because the small,
        # conversational `common_*_knowledge` corpora are noisy under BM25
        # (PG `simple` tsv has no Hebrew analyzer; prefix-OR expansion
        # surfaces too many low-quality hits and dilutes vector ranking
        # under RRF — see A/B on 2026-05-10).
        #
        # Per-context override: long-document corpora where the title is
        # the strongest signal (`government_decisions`, `legal_advisor_*`,
        # `committee_decisions`, `ethics_decisions`, `legal_text`) are the
        # opposite case — `text-embedding-3-small` cosine ranks verbatim-
        # title queries deep in the corpus (rank #13,904 of 29,795 for the
        # 2025-10-26 "פיתוח ושיקום תשתיות ביישובים מוחלשים" probe), while
        # BM25 surfaces the same target at rank #1. Those contexts opt back
        # in via `use_lexical_search: true` in `specs/<bot>/config.yaml`.
        # An explicit context value (true OR false) always beats the mode default.
        ctx_cfg = next(
            (c for c in self.config.get('context', [])
             if c.get('slug') == context_name),
            None,
        )
        ctx_lex = ctx_cfg.get('use_lexical_search') if ctx_cfg else None
        if ctx_lex is None:
            use_lexical = bool(getattr(search_mode, "use_lexical_search", True))
        else:
            use_lexical = bool(ctx_lex)

        # Which lexical scoring path to use when the lexical branch is on.
        # `tsquery` (default) — existing prefix-OR BM25 with ts_rank_cd.
        # `trigram` — pg_trgm.word_similarity() ranking, GIN-indexed.
        # See _LEXICAL_STRATEGIES constants for rationale.
        ctx_strategy = (ctx_cfg or {}).get("lexical_strategy", _LEXICAL_STRATEGY_TSQUERY)
        if ctx_strategy not in _LEXICAL_STRATEGIES:
            logger.warning(
                "context %s: unknown lexical_strategy=%r, falling back to %s",
                context_name, ctx_strategy, _LEXICAL_STRATEGY_TSQUERY,
            )
            ctx_strategy = _LEXICAL_STRATEGY_TSQUERY

        # Query-side law detection: when the model leaves israeli_laws unfiltered but
        # the query names a specific law, detect+resolve it and re-scope (independent of
        # the model's tool-selection). israeli_laws-only; one level deep — the re-entrant
        # call carries law_name, so has_law_name is True there and this block is skipped.
        _q_law = (metadata_filter or {}).get("law_name")
        _q_no_law = _q_law is None or not _normalize_law_name(str(_q_law))
        if _q_no_law and context_name == "israeli_laws" and use_vector and not _qd_skip:
            with get_session() as ds:
                _drow = ds.execute(text(
                    "SELECT id FROM contexts WHERE bot=:bot AND name=:name"
                ), {"bot": bot, "name": context_name}).fetchone()
                detected = (_detect_law_in_query(ds, str(_drow[0]), query_text,
                                                 _QUERY_DETECT_THRESHOLD) if _drow else None)
            if detected:
                logger.info("search query-detected: query=%r resolved=%r (%s, %s)",
                            query_text, detected, bot, context_name)
                df = self.search(context_name, query_text, search_mode, embedding,
                                 num_results=num_results, explain=explain,
                                 metadata_filter={"law_name": detected})
                for hit in df["hits"]["hits"]:
                    hit.setdefault("_source", {}).setdefault("metadata", {})["_query_detected_law"] = detected
                return df

        # Resolve context_id from (bot, name) — small extra round-trip but
        # keeps the search call self-contained and resilient to context
        # rows being added/removed mid-process.
        with get_session() as sess:
            # HNSW `ef_search` per-context — see _HNSW_EF_SEARCH_DEFAULT for
            # rationale. Default 100 trades a small latency hit for recall on
            # small Hebrew corpora; overridable per context in
            # `specs/<bot>/config.yaml`. SET LOCAL keeps the override scoped
            # to this txn so it doesn't leak across the connection pool.
            # (See migration 0007 for the ivfflat → hnsw swap rationale.)
            # Only set when we'll actually use the vector branch — saves a
            # no-op SET on BM25-only modes.
            if use_vector:
                ef = _resolve_int_setting(
                    ctx_cfg, 'hnsw_ef_search', _HNSW_EF_SEARCH_DEFAULT,
                    minimum=_HNSW_EF_SEARCH_MIN, maximum=_HNSW_EF_SEARCH_MAX,
                )
                sess.execute(text(f"SET LOCAL hnsw.ef_search = {ef}"))

            row = sess.execute(text(
                "SELECT id FROM contexts WHERE bot=:bot AND name=:name"
            ), {"bot": bot, "name": context_name}).fetchone()
            if not row:
                logger.warning("search: context (%s, %s) not found", bot, context_name)
                return {"hits": {"hits": []}}
            cid = str(row[0])

            # A law_name filter is a scoping directive ("answer from THIS law").
            # An empty-string law_name (LLM bug) normalizes to "" — treat as no filter.
            law_value = (metadata_filter or {}).get("law_name")
            law_norm = _normalize_law_name(str(law_value)) if law_value is not None else None
            if law_value is not None and not law_norm:
                logger.warning("search: empty law_name filter for (%s, %s); ignoring it", bot, context_name)
                metadata_filter = {k: v for k, v in metadata_filter.items() if k != "law_name"} or None
                law_norm = None
            has_law_name = law_norm is not None
            other_keys = bool({k for k in (metadata_filter or {}) if k != "law_name"})

            md_filter_sql, md_params = _build_metadata_filter_sql(metadata_filter)

            if has_law_name:
                # Scope-preserving recall: exact vector KNN over the law's docs, regardless
                # of the mode's use_vector flag, via a MATERIALIZED CTE (no global HNSW
                # post-filter). Wider fetch than default — the right section may sit deeper
                # than num_results*5 within a multi-hundred-doc law, and a lexical-only mode
                # has no lexical safety net.
                rest_sql, rest_params = _build_metadata_filter_sql(
                    {k: v for k, v in metadata_filter.items() if k != "law_name"})
                scoped_fetch = max(fetch, num_results * 15)
                vector_rows = _scoped_vector_knn(sess, cid, law_norm, rest_sql, rest_params,
                                                 embedding, scoped_fetch)
                # (Observability log is emitted in Task 4, after the lexical branch and the
                # fallback decision, where scoped_lex and gate_fired are also known.)
            elif use_vector:
                vector_rows = sess.execute(text(
                    f"""
                    SELECT id, content, metadata, 1 - (embedding <=> CAST(:emb AS vector)) AS score
                    FROM documents
                    WHERE context_id = :cid{md_filter_sql}
                    ORDER BY embedding <=> CAST(:emb AS vector)
                    LIMIT :limit
                    """
                ), {"cid": cid, "emb": str(embedding), "limit": fetch, **md_params}).fetchall()
            else:
                vector_rows = []

            # Lexical scoring branch. Two strategies, opt-in per context:
            #
            # 1. `tsquery` (default, back-compat): prefix-OR BM25.
            #    Can't use plainto_tsquery directly — it ANDs every term
            #    and uses exact match, both fatal for Hebrew:
            #      - AND fails on stopword-y interrogatives ("מהן", "מה")
            #      - construct/absolute alternation ("ועדת"/"ועדה"/"ועדות")
            #        misses 90%+ of relevant docs under exact match
            #    Mitigation: prefix-OR `term:*` against the weighted
            #    multi-field tsv (migration 0004) via ts_rank_cd.
            #
            # 2. `trigram`: pg_trgm.word_similarity() ranking.
            #    Hebrew-aware via character 3-grams — bridges construct
            #    alternation natively. Index: `documents_content_trgm`
            #    (migration 0015). On the prod query, surfaced 3/3
            #    prod-cited sections in top-8; tsquery hit 0/3.
            if not use_lexical:
                lexical_rows = []
            elif ctx_strategy == _LEXICAL_STRATEGY_TRIGRAM:
                # Lower the threshold so word_similarity returns hits in the
                # 0.1-0.6 range we observed as legitimately relevant.
                sess.execute(text(
                    f"SET LOCAL pg_trgm.word_similarity_threshold = "
                    f"{_TRIGRAM_WORD_SIMILARITY_THRESHOLD}"
                ))
                # `%>` is the word-similarity-above-threshold operator and
                # is GIN-indexable via gin_trgm_ops — short-circuits before
                # word_similarity() runs against every row.
                lexical_rows = sess.execute(text(
                    f"""
                    SELECT id, content, metadata,
                           word_similarity(:q, content) AS score
                    FROM documents
                    WHERE context_id = :cid
                      AND :q %> content{md_filter_sql}
                    ORDER BY score DESC
                    LIMIT :limit
                    """
                ), {"cid": cid, "q": query_text, "limit": fetch, **md_params}).fetchall()
            else:
                ts_query_str = _build_prefix_or_tsquery(query_text)
                if ts_query_str:
                    lexical_rows = sess.execute(text(
                        f"""
                        SELECT id, content, metadata,
                               ts_rank_cd(tsv, to_tsquery('simple', :q)) AS score
                        FROM documents
                        WHERE context_id = :cid
                          AND tsv @@ to_tsquery('simple', :q){md_filter_sql}
                        ORDER BY score DESC
                        LIMIT :limit
                        """
                    ), {"cid": cid, "q": ts_query_str, "limit": fetch, **md_params}).fetchall()
                else:
                    # All tokens too short / stopwords — skip the lexical pass.
                    lexical_rows = []
            bm25_rows = lexical_rows  # kept name for back-compat with _rrf_fuse call below

        # Reduce the scoped vector's weight only when we OVERRODE a lexical-only mode
        # (e.g. SECTION_NUMBER), so an injected scoped-vector hit can't displace an exact
        # §86 lexical match. For modes where vector was already on, weight is unchanged.
        _vw = _SCOPED_OVERRIDE_VECTOR_WEIGHT if (has_law_name and not use_vector) else 1.0
        result = _rrf_fuse(vector_rows, bm25_rows, num_results, vector_weight=_vw)
        # Spec §D observability + scope-preserving fallback. The fallback fires ONLY when
        # law_name is the SOLE filter key and the fully-scoped result is empty — i.e. the
        # named law has zero docs (a genuinely absent colloquial name like "חוק המכרזים").
        # A compound-filter miss returns empty rather than widening to cross-law. Bounded
        # to one level (re-run passes metadata_filter=None). bm25_rows / vector_rows are in
        # function scope here (assigned inside the closed `with get_session()` block).
        gate_fired = bool(has_law_name and not other_keys and not result["hits"]["hits"])
        if has_law_name:
            logger.info("search scoped: law_name=%r scoped_vec=%d scoped_lex=%d gate_fired=%s (%s, %s)",
                        law_norm, len(vector_rows), len(bm25_rows), gate_fired, bot, context_name)
        if gate_fired:
            with get_session() as rs:
                resolved = _resolve_law_name(rs, cid, law_norm, _LAW_NAME_RESOLVE_THRESHOLD)
            if resolved and _normalize_law_name(resolved) != law_norm:
                logger.info("search: law_name=%r resolved to %r in (%s, %s); re-scoping",
                            law_norm, resolved, bot, context_name)
                rf = self.search(context_name, query_text, search_mode, embedding,
                                 num_results=num_results, explain=explain,
                                 metadata_filter={"law_name": resolved})
                for hit in rf["hits"]["hits"]:
                    hit.setdefault("_source", {}).setdefault("metadata", {})["_resolved_from"] = law_norm
                return rf
            fb = self.search(context_name, query_text, search_mode, embedding,
                             num_results=num_results, explain=explain, metadata_filter=None,
                             _qd_skip=True)
            for hit in fb["hits"]["hits"]:
                hit.setdefault("_source", {}).setdefault("metadata", {})["_fallback_reason"] = "law_name_absent"
            return fb
        # Decision-complete retrieval: for the interpretive/decision corpora, hand the
        # LLM the whole decision/opinion a chunk belongs to (same DocumentTitle) instead
        # of a fragment. Opt-in per context; runs on the final fused result; own session
        # (the search session is already closed here). Best-effort inside the helper.
        if ctx_cfg and ctx_cfg.get("expand_to_document") and result["hits"]["hits"]:
            _ecap = _resolve_int_setting(ctx_cfg, "expand_max_chunks", _EXPAND_MAX_CHUNKS_DEFAULT,
                                         minimum=1, maximum=200)
            with get_session() as es:
                result["hits"]["hits"] = _expand_to_documents(es, cid, result["hits"]["hits"], _ecap)
        return result

    def government_distribution(self, context_name: str, decision_number: str) -> list[dict]:
        """One entry per distinct government_number with the given decision_number.
        Returns [] when <2 governments match — callers skip injection in that case.
        """
        bot = self.config.get("slug")
        with get_session() as sess:
            row = sess.execute(text(
                "SELECT id FROM contexts WHERE bot=:bot AND name=:name"
            ), {"bot": bot, "name": context_name}).fetchone()
            if not row:
                logger.warning("government_distribution: context (%s, %s) not found", bot, context_name)
                return []
            cid = str(row[0])
            rows = sess.execute(text(r"""
                SELECT
                    metadata->>'government_number'          AS government_number,
                    metadata->>'government'                 AS government,
                    COUNT(*)                                AS doc_count,
                    MAX(CASE
                        WHEN metadata->>'publish_date' ~ E'^\\d{2}\\.\\d{2}\\.\\d{4}$'
                            THEN to_date(metadata->>'publish_date', 'DD.MM.YYYY')
                        WHEN metadata->>'publish_date' ~ E'^\\d{4}-\\d{2}-\\d{2}'
                            THEN (metadata->>'publish_date')::date
                        END)                                    AS latest_publish_date
                FROM documents
                WHERE context_id = :cid
                  AND metadata @> CAST(:mfilter AS jsonb)
                  AND metadata->>'government_number' IS NOT NULL
                GROUP BY metadata->>'government_number', metadata->>'government'
                ORDER BY CASE WHEN metadata->>'government_number' ~ '^\d+$'
                              THEN (metadata->>'government_number')::int END NULLS LAST
            """), {"cid": cid, "mfilter": json.dumps({"decision_number": decision_number})}).fetchall()
        if len(rows) < 2:
            return []
        return [{"government_number": r[0], "government": r[1],
                 "doc_count": r[2], "latest_publish_date": r[3]} for r in rows]

    def update_tools(self, context_, vector_store):
        """Emit an OpenAI function-tool definition for this context.
        Uses the context_name (not the uuid) as the tool-name suffix so
        the LLM sees the same tool names as today's ES backend.
        """
        from .search_modes import SEARCH_MODES, DEFAULT_SEARCH_MODE
        context_name = context_.get("slug", "unknown")
        tool_description = self._tool_description(context_)
        search_mode_description = self._search_mode_description(context_)

        self.tools.append({
            "type": "function",
            "function": {
                "name": f"search_{context_name}",
                "description": tool_description,
                "parameters": {
                    "type": "object",
                    "properties": {
                        "query": {
                            "type": "string",
                            "description": "The query string to use for semantic/free text search",
                        },
                        "search_mode": {
                            "type": "string",
                            "description": search_mode_description,
                            "enum": [mode.name for mode in SEARCH_MODES.values()],
                            "default": DEFAULT_SEARCH_MODE.name,
                        },
                        "num_results": {
                            "type": "integer",
                            "description": "Number of results to return. Leave empty to use the default for the search mode.",
                            "default": 7,
                        },
                    },
                    "required": ["query"],
                },
            },
        })

    def update_tool_resources(self, context_, vector_store):
        """Aurora doesn't use OpenAI tool_resources (those are for OpenAI's
        own vector stores). Mirror the ES backend by setting None."""
        self.tool_resources = None

    # Helpers — copied verbatim from VectorStoreES so behavior is identical
    def _tool_description(self, context_) -> str:
        description = context_.get("description", "")
        examples = context_.get("examples", "")
        if description and examples:
            return f"{description}. Examples: {examples}"
        if description:
            return description
        context_name = context_.get("slug", "unknown")
        return f"Semantic search the '{context_name}' vector store"

    def _search_mode_description(self, context_) -> str:
        base = "Search mode. "
        slug = context_.get("slug", "")
        if any(k in slug for k in ("legal_text", "common_knowledge")):
            modes = [
                "'SECTION_NUMBER': Specialized search for finding legal text sections by their number "
                "(e.g. 'סעיף 12'). Requires both section number and resource name (default 3 results)",
                "'REGULAR': Semantic + full text search across all main fields (default 7 results)",
                "'METADATA_BROWSE': Browse documents with structured metadata summaries instead of full content (25 results)",
            ]
        elif any(k in slug for k in ("legal_advisor_opinions", "legal_advisor_letters",
                                      "committee_decisions", "ethics_decisions")):
            modes = [
                "'METADATA_BROWSE': Browse documents with structured metadata summaries instead of full content (25 results)",
                "'REGULAR': Semantic + full text search across all main fields (7 results)",
            ]
        else:
            modes = [
                "'REGULAR': Semantic + full text search across all main fields (default 7 results)",
                "'METADATA_BROWSE': Browse documents with structured metadata summaries instead of full content (25 results)",
            ]
        return base + ". ".join(modes) + "."


# Hebrew interrogatives + common particles that contribute no retrieval
# signal and pollute prefix expansion. Not a true stopword list — just
# tokens that show up in user queries but never identify a topic.
_TS_QUERY_DROP = frozenset({
    "מה", "מהן", "מהו", "מהי", "מי", "האם", "כיצד", "איך", "למה", "מדוע",
    "איפה", "מתי", "האם",
    "של", "את", "על", "אל", "כי", "או", "גם", "כמו", "אם", "לא", "כן",
    "the", "a", "an", "of", "is", "in", "to", "and", "or", "for", "what",
    "how", "why", "when", "where", "who", "which",
})

# Token-count threshold for _build_prefix_or_tsquery's long-query
# fallback. Queries with more than this many post-filter tokens skip
# the prefix-OR + stem-variant expansion and use AND-only on bare
# tokens instead — see the rationale block in _build_prefix_or_tsquery.
_MAX_PREFIX_TOKENS = 6


def _build_prefix_or_tsquery(query_text: str) -> str:
    """Turn a user query into a tsquery string with OR + prefix semantics.

    Example: "מהן סמכויות ועדת הכנסת" → "סמכויות:* | ועדת:* | הכנסת:*"

    - Drops tokens shorter than 2 chars (no useful prefix).
    - Drops the small interrogative/particle set in `_TS_QUERY_DROP`.
    - Escapes any tsquery operator characters so user input can't break the
      query (`& | ! ( ) :` are stripped).
    - Returns empty string if no usable tokens remain — caller should skip
      BM25 in that case rather than crash to_tsquery.
    """
    # Strip everything that isn't a letter (any script — covers Hebrew,
    # Arabic, Latin) or digit. This nukes both tsquery operators
    # (`& | ! ( ) :`) and ordinary punctuation (`? . , ;`) that would
    # otherwise become part of a token and cause prefix matches to fail.
    cleaned = re.sub(r"[^\w\s]", " ", query_text, flags=re.UNICODE)
    # Token length floor of 3 (was 2): a 2-char prefix matches too many docs
    # to provide signal. Numeric tokens additionally need length >= 4 — a
    # 3-digit prefix like "202:*" matches every year 2020–2029 plus any
    # other "202..." token, which polluted the BM25 ranking and crowded
    # out instruction-bearing docs in the budget knowledge corpus.
    def _ok(t: str) -> bool:
        if t.lower() in _TS_QUERY_DROP:
            return False
        if t.isdigit():
            return len(t) >= 4
        return len(t) >= 3
    tokens = [t for t in cleaned.split() if _ok(t)]
    if not tokens:
        return ""

    # Long-query fallback: a 10-term prefix-OR (~20-way after the
    # stem-variant expansion below) blew past the 12s RETRIEVE_TIMEOUT
    # on staging Aurora against the ~30k-row government_decisions
    # corpus (verbatim-title query, probed 2026-05-12).
    #
    # For long queries we switch from prefix-OR to AND-with-prefix:
    #   - AND is naturally selective — Postgres picks the most-
    #     selective prefix match first, so the overall scan stays
    #     cheap even on large corpora (a single distinctive term
    #     narrows the candidate set to a handful of rows).
    #   - Keeping the `:*` prefix on each term preserves morphology
    #     coverage (Hebrew construct/absolute alternation, plural
    #     suffixes, etc.) — pure AND-on-bare-tokens regressed
    #     government_decisions on the local probe (5 random titles:
    #     80% → 40%) because some title tokens had inflected forms
    #     in the doc that exact-match couldn't catch.
    #   - Skipping the stem-variant expansion (the second `:*` per
    #     token) keeps the plan width bounded.
    # A cap-and-truncate alternative (keep top-N tokens by length)
    # was tested first and regressed local probe set 93% → 72% —
    # it dropped distinctive short proper-noun tokens (e.g. MK names
    # in ethics-decisions titles) that BM25 needed to rank correctly.
    # AND-with-prefix below keeps every token.
    if len(tokens) > _MAX_PREFIX_TOKENS:
        return " & ".join(f"{t}:*" for t in tokens)

    # For each token emit prefix variants:
    #   - the token itself with `:*` (matches the exact form + suffixes)
    #   - for non-numeric tokens with len >= 5, the token minus its last
    #     character with `:*` (covers Hebrew construct/absolute alternation:
    #     "ועדת" → "ועד:*" matches "ועדה" / "ועדות"). Numeric tokens never
    #     get this stem variant — "2026"[:-1] = "202" would match other
    #     years and is exactly the noise we just filtered out above.
    parts: list[str] = []
    seen: set[str] = set()
    for t in tokens:
        variants = [t]
        if not t.isdigit() and len(t) >= 5:
            variants.append(t[:-1])
        for stem in variants:
            if stem not in seen:
                seen.add(stem)
                parts.append(f"{stem}:*")
    return " | ".join(parts)


def _rrf_fuse(
    vector_rows: list,
    bm25_rows: list,
    num_results: int,
    k: int = 60,
    bm25_weight: float = 3.0,
    vector_weight: float = 1.0,
) -> dict:
    """Weighted reciprocal-rank-fusion.

    Standard RRF (`1/(k + rank + 1)`) gives equal weight to vector and BM25.
    For Hebrew, where the query embedding and the lexical signal frequently
    point at different chunks, equal-weight RRF leaves disjoint rank-1
    results tied at `1/(k+1)` — and the dict-insertion-order tiebreaker
    silently hands all top slots to the side that was inserted first.

    We give BM25 (the rebuilt prefix-OR-stem-expanded query against the
    weighted multi-field tsv from migration 0004) more weight because:
      - DocumentTitle weight A in the tsv reliably surfaces relevant docs
        when the query hits a title term.
      - Vector cosine on `text-embedding-3-small` is noisy for Hebrew with
        no language-specific tuning — many irrelevant docs get high cosine
        just from sharing common words like "הכנסת".
    BM25 weight is 3× as of 2026-04-28 (was 2×): on the budget knowledge
    corpus the over-fetched vector list (5× num_results) was repeatedly
    drowning out BM25's correct top-1 even at 2× — bumping to 3× lets the
    lexical signal win when it's confident. Set both weights to 1.0 to
    recover canonical RRF.
    """
    try:
        from opentelemetry import trace as otel_trace
        tracer = otel_trace.get_tracer(__name__)
    except ImportError:
        tracer = None

    def _do(_span):
        if _span is not None:
            _span.set_attribute("rrf.vector_candidates", len(vector_rows))
            _span.set_attribute("rrf.bm25_candidates", len(bm25_rows))
            _span.set_attribute("rrf.num_results", num_results)

        scores: dict[str, float] = {}
        docs: dict[str, tuple] = {}

        for rank, row in enumerate(vector_rows):
            doc_id = str(row[0])
            scores[doc_id] = scores.get(doc_id, 0.0) + vector_weight / (k + rank + 1)
            docs[doc_id] = row

        for rank, row in enumerate(bm25_rows):
            doc_id = str(row[0])
            scores[doc_id] = scores.get(doc_id, 0.0) + bm25_weight / (k + rank + 1)
            docs.setdefault(doc_id, row)

        ordered = sorted(scores.items(), key=lambda kv: kv[1], reverse=True)[:num_results]

        hits = []
        for doc_id, fused_score in ordered:
            row = docs[doc_id]
            hits.append({
                "_id": doc_id,
                "_score": fused_score,
                "_source": {
                    "content": row[1],
                    "metadata": row[2] if isinstance(row[2], dict) else json.loads(row[2]),
                },
            })
        return {"hits": {"hits": hits}}

    if tracer is None:
        return _do(None)
    with tracer.start_as_current_span("rrf.fuse") as span:
        result = _do(span)
        try: span.set_attribute("rrf.returned", len(result["hits"]["hits"]))
        except Exception: pass
        return result


_EXPAND_MAX_CHUNKS_DEFAULT = 12
_EXPAND_TOTAL_CHUNKS_BUDGET = 40  # aggregate cap across all hits in one result (bounds tool-response size)


def _expand_to_documents(sess, cid, hits, max_chunks=_EXPAND_MAX_CHUNKS_DEFAULT, total_budget=_EXPAND_TOTAL_CHUNKS_BUDGET):
    """Replace each hit's content with the FULL decision/opinion it belongs to
    (all chunks sharing its metadata.DocumentTitle, chunk_index-ordered, capped at
    max_chunks), so the LLM reasons over the complete finding instead of a fragment.
    Hits sharing a DocumentTitle collapse to the first (highest-ranked) one; hits
    without a DocumentTitle pass through unchanged. Best-effort: on any error, return
    the original hits.

    total_budget caps the AGGREGATE kept-chunk count across ALL expanded hits in one
    call. Once used >= total_budget, remaining titled hits pass through un-expanded
    (in their original RRF position) instead of ballooning the tool response.
    Hits that pass through (no title, already-seen dedup, or budget-spent) do NOT
    count against the budget.
    """
    try:
        titles = []
        for h in hits:
            t = (h.get("_source", {}).get("metadata", {}) or {}).get("DocumentTitle")
            if t and t not in titles:
                titles.append(t)
        if not titles:
            return hits
        stmt = text(
            "SELECT metadata->>'DocumentTitle' AS t, content, "
            "COALESCE((metadata->>'chunk_index')::int, 0) AS ci "
            "FROM documents "
            "WHERE context_id = :cid AND metadata->>'DocumentTitle' IN :titles "
            "ORDER BY t, ci, id"
        ).bindparams(bindparam("titles", expanding=True))
        by_title = {}
        for row in sess.execute(stmt, {"cid": cid, "titles": titles}).fetchall():
            by_title.setdefault(row[0], []).append(row[1])
        out, seen = [], set()
        used = 0
        for h in hits:
            t = (h.get("_source", {}).get("metadata", {}) or {}).get("DocumentTitle")
            if not t or t not in by_title:
                out.append(h)
                continue
            if t in seen:
                continue
            seen.add(t)
            if used >= total_budget:
                out.append(h)
                continue
            chunks = by_title[t]
            kept = chunks[:max_chunks]
            used += len(kept)
            new_h = {**h, "_source": {**h["_source"],
                     "content": "\n\n".join(c for c in kept if c),
                     "metadata": {**h["_source"].get("metadata", {}),
                                  "_expanded_chunks": len(kept),
                                  "_expanded_truncated": len(chunks) > max_chunks}}}
            out.append(new_h)
        return out
    except Exception as e:  # noqa: BLE001 — expansion must never fail the search
        logger.warning("decision-complete expansion skipped: %s", e)
        return hits
