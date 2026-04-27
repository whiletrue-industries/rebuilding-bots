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
from sqlalchemy import text

from ..config import is_production, get_logger, DEFAULT_EMBEDDING_SIZE, DEFAULT_EMBEDDING_MODEL
from ..db.session import get_engine, get_session
from .vector_store_base import VectorStoreBase

logger = get_logger(__name__)


# Targets for content chunking when a source doc exceeds the embedding model's
# token limit. text-embedding-3-{small,large} both cap at 8192. We chunk at
# 6000 with 300 overlap to leave headroom for tokenizer drift between tiktoken's
# estimate and OpenAI's actual tokenizer (the two are very close for cl100k_base
# but not bit-identical), and to keep retrieval-quality high (chunks ≤ ~6k tokens
# represent a single concept tightly; longer chunks average too much).
CHUNK_MAX_TOKENS = 6000
CHUNK_OVERLAP_TOKENS = 300
EMBEDDING_TOKEN_LIMIT = 8192  # OpenAI text-embedding-3-* hard ceiling

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
    api_key = (
        os.getenv("OPENAI_API_KEY_PRODUCTION")
        if environment == "production"
        else os.getenv("OPENAI_API_KEY_STAGING")
    )
    client = OpenAI(api_key=api_key)

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
        # to mint query embeddings. Same env-var convention as VectorStoreES; the
        # CLAUDE.md troubleshooting note covers why we read STAGING for non-prod
        # (no separate _LOCAL key in the existing infra).
        openai_api_key = (
            os.getenv("OPENAI_API_KEY_PRODUCTION") if production
            else os.getenv("OPENAI_API_KEY_STAGING")
        )
        self.openai_client = OpenAI(api_key=openai_api_key)

        # Trigger engine creation early so connection failures surface here, not later
        get_engine()
        logger.info("VectorStoreAurora initialized for environment=%s", env_name)

    # ---- abstract method overrides -----------------------------------------

    def get_or_create_vector_store(self, context, context_name, replace_context):
        """Return the context_id (uuid str) for (bot, context_name).

        - Inserts a row into contexts if it doesn't exist.
        - If replace_context is True, deletes all rows in documents that
          reference this context (CASCADE handles the join).
        """
        bot = self.config["slug"]
        with get_session() as sess:
            row = sess.execute(text(
                "INSERT INTO contexts (bot, name) VALUES (:bot, :name) "
                "ON CONFLICT (bot, name) DO UPDATE SET updated_at=now() "
                "RETURNING id"
            ), {"bot": bot, "name": context_name}).fetchone()
            cid = str(row[0])

            if replace_context:
                sess.execute(text(
                    "DELETE FROM documents WHERE context_id = :cid"
                ), {"cid": cid})
                logger.info("Cleared documents for context %s/%s (id=%s)", bot, context_name, cid)
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

        with get_session() as sess:
            for fname, content_file, file_type, metadata in file_streams:
                if not fname.endswith(".md"):
                    logger.debug("Skipping non-markdown file: %s", fname)
                    continue
                files_seen += 1

                try:
                    raw_content = content_file.read().decode("utf-8")
                except Exception as exc:
                    logger.error("Failed to read file %s: %s", fname, exc)
                    skipped += 1
                    continue

                chunks = _chunk_for_embedding(raw_content)
                total_chunks = len(chunks)
                if total_chunks > 1:
                    logger.info(
                        "Chunked %s into %d pieces (oversize content)",
                        fname, total_chunks,
                    )

                for chunk_index, chunk_content in enumerate(chunks):
                    try:
                        chunk_hash = hashlib.sha256(chunk_content.encode("utf-8")).hexdigest()

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
                    except Exception as exc:
                        logger.error(
                            "Failed to process %s (chunk %d/%d): %s",
                            fname, chunk_index + 1, total_chunks, exc,
                        )
                        skipped += 1
                        continue

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
        """Delete documents whose metadata.filename matches any in file_names.
        Returns the count of deleted rows.
        """
        cid = vector_store
        with get_session() as sess:
            result = sess.execute(text(
                "DELETE FROM documents "
                "WHERE context_id = :cid AND metadata->>'filename' = ANY(:names)"
            ), {"cid": cid, "names": list(file_names)})
            return result.rowcount

    def search(
        self,
        context_name: str,
        query_text: str,
        search_mode,           # SearchModeConfig — kept for ES-parity signature
        embedding: list[float],
        num_results: int = 7,
        explain: bool = False,
        metadata_filter: dict | None = None,
    ) -> dict:
        """Hybrid retrieval: pgvector cosine + tsvector BM25, fused via
        reciprocal-rank-fusion. Mirrors VectorStoreES.search's return shape
        so downstream code (search_modes.py, the LLM tool layer) doesn't
        need to know which backend it talked to.

        Returns: {"hits": {"hits": [{"_id", "_score", "_source": {...}}, ...]}}
        """
        bot = self.config["slug"]
        fetch = num_results * 5  # over-fetch then RRF-trim

        # Resolve context_id from (bot, name) — small extra round-trip but
        # keeps the search call self-contained and resilient to context
        # rows being added/removed mid-process.
        with get_session() as sess:
            # ivfflat default `probes = 1` searches only 1 of `lists` partitions,
            # which makes top-K depend on which partition the query embedding
            # lands in — different ivfflat builds (e.g. local vs staging
            # rebuilt at slightly different times) can return materially
            # different rankings even for the same query+data. Bumping probes
            # to 10 trades a small latency hit for deterministic, near-exact
            # top-K. We use SET LOCAL so the change is scoped to this txn
            # and doesn't leak across the connection pool.
            sess.execute(text("SET LOCAL ivfflat.probes = 10"))

            row = sess.execute(text(
                "SELECT id FROM contexts WHERE bot=:bot AND name=:name"
            ), {"bot": bot, "name": context_name}).fetchone()
            if not row:
                logger.warning("search: context (%s, %s) not found", bot, context_name)
                return {"hits": {"hits": []}}
            cid = str(row[0])

            md_filter_sql = ""
            md_params = {}
            if metadata_filter:
                md_filter_sql = " AND metadata @> CAST(:mfilter AS jsonb)"
                md_params["mfilter"] = json.dumps(metadata_filter)

            vector_rows = sess.execute(text(
                f"""
                SELECT id, content, metadata, 1 - (embedding <=> CAST(:emb AS vector)) AS score
                FROM documents
                WHERE context_id = :cid{md_filter_sql}
                ORDER BY embedding <=> CAST(:emb AS vector)
                LIMIT :limit
                """
            ), {"cid": cid, "emb": str(embedding), "limit": fetch, **md_params}).fetchall()

            # BM25 query construction. We can't use plainto_tsquery directly
            # because it ANDs every term and treats words as exact matches —
            # both fatal for Hebrew search:
            #   - AND fails when the user query has a stopword-y interrogative
            #     ("מהן", "מה", "האם") that appears in zero documents.
            #   - Hebrew has heavy construct/absolute form alternation
            #     ("ועדת" vs "ועדה" vs "ועדות") with no PG-shipped Hebrew
            #     analyzer, so exact matching misses 90%+ of relevant docs.
            # Mitigation: convert each non-trivial token to a `term:*` prefix
            # match and OR them. This roughly mirrors what ES did with
            # `fuzziness: AUTO` on REGULAR_CONFIG. Combined with the weighted
            # multi-field tsv (migration 0004) and ts_rank_cd, the BM25 side
            # surfaces docs whose DocumentTitle / metadata mentions the topic
            # even when the exact construct form isn't in the body.
            ts_query_str = _build_prefix_or_tsquery(query_text)
            if ts_query_str:
                bm25_rows = sess.execute(text(
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
                # Empty query (all tokens too short / stopwords); skip BM25.
                bm25_rows = []

        return _rrf_fuse(vector_rows, bm25_rows, num_results)

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
    tokens = [t for t in cleaned.split() if len(t) >= 2 and t.lower() not in _TS_QUERY_DROP]
    if not tokens:
        return ""
    # For each token emit two prefix variants:
    #   - the token itself with `:*` (matches the exact form + suffixes)
    #   - the token with its last char dropped + `:*` (covers Hebrew
    #     construct/absolute alternation: "ועדת" → "ועד:*" matches
    #     "ועדה" / "ועדות" / "ועד..."; same trick works for verb roots).
    # We OR everything. Stems shorter than 3 chars don't get the extra
    # variant — too noisy (would match every word with a 2-char prefix).
    parts: list[str] = []
    seen: set[str] = set()
    for t in tokens:
        for stem in (t, t[:-1] if len(t) >= 4 else None):
            if stem and stem not in seen:
                seen.add(stem)
                parts.append(f"{stem}:*")
    return " | ".join(parts)


def _rrf_fuse(
    vector_rows: list,
    bm25_rows: list,
    num_results: int,
    k: int = 60,
    bm25_weight: float = 2.0,
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
    The 2× weight is empirically chosen against the deploy/gold-set; it's
    not a hard guarantee. Set both weights to 1.0 to recover canonical RRF.
    """
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
