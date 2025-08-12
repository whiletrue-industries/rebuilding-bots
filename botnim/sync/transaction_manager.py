"""
Transactional safety helpers for Elasticsearch operations.

Implements a best-effort two-phase style flow for index updates:
1) Write new version with a composite document_id (source_id + version_hash)
2) Mark old documents for the same source_id as stale
3) Delete stale documents in a separate, retryable step

This avoids inconsistent mixed versions when partial failures occur.
"""

from __future__ import annotations

from typing import Dict, Optional

from ..config import get_logger
from ..vector_store.vector_store_es import VectorStoreES
from .resilience import RetryPolicy, CircuitBreaker, with_retry


logger = get_logger(__name__)


class TransactionManager:
    def __init__(self, vector_store: VectorStoreES, *, retry_policy: Optional[RetryPolicy] = None, circuit_breaker: Optional[CircuitBreaker] = None):
        self.vector_store = vector_store
        self.retry_policy = retry_policy or RetryPolicy(max_attempts=3, base_delay_seconds=1.0, max_delay_seconds=16.0)
        self.circuit_breaker = circuit_breaker or CircuitBreaker()

    def upsert_new_version(self, index_name: str, document_id: str, document: Dict) -> str:
        def _op():
            resp = self.vector_store.es_client.index(index=index_name, id=document_id, document=document)
            return resp.get("_id", document_id)

        return with_retry(_op, policy=self.retry_policy, circuit_breaker=self.circuit_breaker, circuit_key=f"es_index:{index_name}")  # type: ignore

    def mark_outdated(self, index_name: str, source_id: str, current_timestamp_iso: str) -> int:
        """
        Mark older docs (same source_id with timestamp < current) as stale=true.
        """
        def _op():
            script = {
                "source": "ctx._source.stale = true",
                "lang": "painless",
            }
            query = {
                "bool": {
                    "must": [
                        {"term": {"source_id": source_id}},
                        {"range": {"timestamp": {"lt": current_timestamp_iso}}},
                    ]
                }
            }
            resp = self.vector_store.es_client.update_by_query(index=index_name, query=query, script=script, conflicts="proceed", refresh=True)
            return int(resp.get("updated", 0))

        return with_retry(_op, policy=self.retry_policy, circuit_breaker=self.circuit_breaker, circuit_key=f"es_update_by_query:{index_name}")  # type: ignore

    def delete_outdated(self, index_name: str, source_id: str, current_timestamp_iso: str) -> int:
        """
        Delete documents for source_id with timestamp < current.
        """
        def _op():
            query = {
                "bool": {
                    "must": [
                        {"term": {"source_id": source_id}},
                        {"range": {"timestamp": {"lt": current_timestamp_iso}}},
                    ]
                }
            }
            resp = self.vector_store.es_client.delete_by_query(index=index_name, query=query, conflicts="proceed", refresh=True)
            return int(resp.get("deleted", 0))

        return with_retry(_op, policy=self.retry_policy, circuit_breaker=self.circuit_breaker, circuit_key=f"es_delete_by_query:{index_name}")  # type: ignore

