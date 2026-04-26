"""Aurora ↔ ES parity check.

Run nightly during the verification window; runs the canonical query
set through both backends, asserts:
    - top-5 doc-ID Jaccard ≥ 0.8
    - aurora p95 latency ≤ es p95 × 1.2

CLI:
    python -m botnim.benchmark.aurora_parity_check --env staging
"""
from __future__ import annotations

import argparse
import json
import math
import sys
import time
from typing import Iterable

# These are the queries that gate cutover. Add here, do NOT add elsewhere.
PARITY_QUERIES: list[str] = [
    "מי הוא נציב קבילות הציבור למקצועות רפואיים במשרד הבריאות?",
    # TODO during implementation: import existing benchmark questions
    # from botnim/benchmark/runner.py and append here.
]

JACCARD_THRESHOLD = 0.8
LATENCY_RATIO_THRESHOLD = 1.2


def jaccard_top_k(a: list[str], b: list[str], k: int) -> float:
    set_a, set_b = set(a[:k]), set(b[:k])
    if not set_a and not set_b:
        return 1.0
    return len(set_a & set_b) / len(set_a | set_b)


def p95_latency(samples: Iterable[float]) -> float | None:
    sample_list = sorted(samples)
    if not sample_list:
        return None
    idx = math.ceil(0.95 * len(sample_list)) - 1
    return sample_list[max(idx, 0)]


def parity_verdict(*, jaccard: float, es_p95: float, aurora_p95: float) -> dict:
    jaccard_ok = jaccard >= JACCARD_THRESHOLD
    latency_ok = aurora_p95 <= es_p95 * LATENCY_RATIO_THRESHOLD
    return {
        "pass": jaccard_ok and latency_ok,
        "jaccard": jaccard,
        "jaccard_ok": jaccard_ok,
        "es_p95_ms": es_p95,
        "aurora_p95_ms": aurora_p95,
        "latency_ok": latency_ok,
    }


def _run_query(backend: str, env: str, query: str) -> tuple[list[str], float]:
    """Issue one search via sync.py's backend factory. Returns (top_ids, latency_ms).

    Note: this is the IO layer — exercised by CI integration, not by the
    unit tests in test_aurora_parity_check.py.
    """
    # Lazy imports so unit tests don't need the full backend stack
    from botnim.config import get_logger
    from botnim.vector_store import VectorStoreES, VectorStoreAurora
    from botnim.vector_store.search_modes import DEFAULT_SEARCH_MODE
    logger = get_logger(__name__)

    config = {"slug": "unified", "name": "Unified"}  # tune per actual bot in CI

    if backend == "es":
        store = VectorStoreES(config, ".", environment=env)
    elif backend == "aurora":
        store = VectorStoreAurora(config, ".", environment=env)
    else:
        raise ValueError(backend)

    # Embed via the same client both backends use
    from botnim.config import get_async_openai_client
    from botnim.config import DEFAULT_EMBEDDING_MODEL
    import asyncio

    async def _embed():
        client = get_async_openai_client(env)
        r = await client.embeddings.create(input=query, model=DEFAULT_EMBEDDING_MODEL)
        return r.data[0].embedding

    embedding = asyncio.run(_embed())

    t0 = time.perf_counter()
    results = store.search(
        context_name="legal_text",
        query_text=query,
        search_mode=DEFAULT_SEARCH_MODE,
        embedding=embedding,
        num_results=5,
    )
    latency_ms = (time.perf_counter() - t0) * 1000

    ids = [hit["_id"] for hit in results["hits"]["hits"]]
    return ids, latency_ms


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--env", choices=["staging", "production"], required=True)
    args = p.parse_args(argv)

    es_latencies: list[float] = []
    aurora_latencies: list[float] = []
    jaccards: list[float] = []

    for q in PARITY_QUERIES:
        es_ids, es_ms = _run_query("es", args.env, q)
        aurora_ids, aurora_ms = _run_query("aurora", args.env, q)
        es_latencies.append(es_ms)
        aurora_latencies.append(aurora_ms)
        jaccards.append(jaccard_top_k(es_ids, aurora_ids, k=5))

    avg_jaccard = sum(jaccards) / len(jaccards) if jaccards else 0.0
    verdict = parity_verdict(
        jaccard=avg_jaccard,
        es_p95=p95_latency(es_latencies) or 0,
        aurora_p95=p95_latency(aurora_latencies) or 0,
    )
    print(json.dumps(verdict, indent=2))
    return 0 if verdict["pass"] else 1


if __name__ == "__main__":
    sys.exit(main())
