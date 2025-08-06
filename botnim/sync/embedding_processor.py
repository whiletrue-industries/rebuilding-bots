"""
Content Embedding and Vectorization for Automated Sync System

This module provides:
1. Cloud-based embedding storage using Elasticsearch
2. Change detection for documents that need re-embedding
3. Batch processing for efficient embedding generation
4. Integration with sync workflow's versioning system
5. Download/upload mechanisms for embedding cache
"""

import hashlib
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple, Union
from dataclasses import dataclass

from openai import OpenAI

from ..config import get_logger, DEFAULT_EMBEDDING_MODEL, DEFAULT_EMBEDDING_SIZE, DEFAULT_BATCH_SIZE
from .config import VersionInfo
from .cache import SyncCache
from ..vector_store.vector_store_es import VectorStoreES
from elasticsearch.helpers import bulk


logger = get_logger(__name__)


@dataclass
class EmbeddingInfo:
    """Information about a document embedding."""
    content_hash: str
    embedding_vector: List[float]
    model: str
    created_at: datetime
    source_id: str
    content_size: int
    metadata: Dict[str, Any]


@dataclass
class EmbeddingBatch:
    """Batch of documents for embedding processing."""
    batch_id: str
    documents: List[Dict[str, Any]]
    batch_size: int
    created_at: datetime
    status: str = 'pending'  # pending, processing, completed, failed


class CloudEmbeddingStorage:
    """
    Elasticsearch-based embedding storage for persistent cloud storage.
    """
    
    def __init__(self, vector_store: VectorStoreES, index_prefix: str = "sync_embeddings"):
        """
        Initialize cloud embedding storage.
        
        Args:
            vector_store: Vector store instance for Elasticsearch access
            index_prefix: Prefix for embedding indices
        """
        self.vector_store = vector_store
        self.es_client = vector_store.es_client
        self.index_prefix = index_prefix
        self.logger = get_logger(__name__)
        
        # Create embedding storage index
        self._ensure_embedding_index_exists()
    
    def _ensure_embedding_index_exists(self):
        """Ensure the embedding storage index exists."""
        index_name = f"{self.index_prefix}_cache"
        
        try:
            if not self.es_client.indices.exists(index=index_name):
                mapping = {
                    "mappings": {
                        "properties": {
                            "content_hash": {"type": "keyword"},
                            "embedding_vector": {
                                "type": "dense_vector",
                                "dims": DEFAULT_EMBEDDING_SIZE
                            },
                            "model": {"type": "keyword"},
                            "created_at": {"type": "date"},
                            "source_id": {"type": "keyword"},
                            "content_size": {"type": "integer"},
                            "metadata": {"type": "object"},
                            "version_hash": {"type": "keyword"},
                            "content_preview": {"type": "text"},
                            "sync_timestamp": {"type": "date"}
                        }
                    },
                    "settings": {
                        "number_of_shards": 1,
                        "number_of_replicas": 0
                    }
                }
                
                self.es_client.indices.create(index=index_name, body=mapping)
                self.logger.info(f"Created embedding storage index: {index_name}")
        
        except Exception as e:
            self.logger.error(f"Failed to ensure embedding index exists: {e}")
    
    def store_embedding(self, embedding_info: EmbeddingInfo) -> bool:
        """
        Store embedding in Elasticsearch.
        
        Args:
            embedding_info: Embedding information to store
            
        Returns:
            True if successful, False otherwise
        """
        try:
            index_name = f"{self.index_prefix}_cache"
            
            document = {
                "content_hash": embedding_info.content_hash,
                "embedding_vector": embedding_info.embedding_vector,
                "model": embedding_info.model,
                "created_at": embedding_info.created_at.isoformat(),
                "source_id": embedding_info.source_id,
                "content_size": embedding_info.content_size,
                "metadata": embedding_info.metadata,
                "sync_timestamp": datetime.now(timezone.utc).isoformat()
            }
            
            # Use content hash as document ID for deduplication
            doc_id = embedding_info.content_hash
            
            self.es_client.index(
                index=index_name,
                id=doc_id,
                body=document
            )
            
            self.logger.debug(f"Stored embedding for content hash: {embedding_info.content_hash[:16]}...")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to store embedding: {e}")
            return False
    
    def get_embedding(self, content_hash: str) -> Optional[EmbeddingInfo]:
        """
        Retrieve embedding from Elasticsearch.
        
        Args:
            content_hash: Content hash to lookup
            
        Returns:
            EmbeddingInfo if found, None otherwise
        """
        try:
            index_name = f"{self.index_prefix}_cache"
            
            response = self.es_client.get(
                index=index_name,
                id=content_hash,
                ignore=[404]
            )
            
            if response.get('found', False):
                source = response['_source']
                return EmbeddingInfo(
                    content_hash=source['content_hash'],
                    embedding_vector=source['embedding_vector'],
                    model=source['model'],
                    created_at=datetime.fromisoformat(source['created_at']),
                    source_id=source['source_id'],
                    content_size=source['content_size'],
                    metadata=source['metadata']
                )
            
            return None
            
        except Exception as e:
            self.logger.error(f"Failed to get embedding for {content_hash}: {e}")
            return None
    
    def batch_store_embeddings(self, embeddings: List[EmbeddingInfo]) -> int:
        """
        Store multiple embeddings in batch.
        
        Args:
            embeddings: List of embeddings to store
            
        Returns:
            Number of successfully stored embeddings
        """
        if not embeddings:
            return 0
        
        try:
            index_name = f"{self.index_prefix}_cache"
            
            # Prepare bulk operations
            actions = []
            for embedding_info in embeddings:
                action = {
                    "_index": index_name,
                    "_id": embedding_info.content_hash,
                    "_source": {
                        "content_hash": embedding_info.content_hash,
                        "embedding_vector": embedding_info.embedding_vector,
                        "model": embedding_info.model,
                        "created_at": embedding_info.created_at.isoformat(),
                        "source_id": embedding_info.source_id,
                        "content_size": embedding_info.content_size,
                        "metadata": embedding_info.metadata,
                        "sync_timestamp": datetime.now(timezone.utc).isoformat()
                    }
                }
                actions.append(action)
            
            # Execute bulk operation
            success_count, failed_items = bulk(
                self.es_client,
                actions,
                index=index_name,
                raise_on_error=False
            )
            
            self.logger.info(f"Bulk stored {success_count} embeddings, {len(failed_items)} failed")
            return success_count
            
        except Exception as e:
            self.logger.error(f"Failed to bulk store embeddings: {e}")
            return 0
    
    def cleanup_old_embeddings(self, days_old: int = 90) -> int:
        """
        Clean up old embeddings from storage.
        
        Args:
            days_old: Number of days after which embeddings are considered old
            
        Returns:
            Number of deleted embeddings
        """
        try:
            index_name = f"{self.index_prefix}_cache"
            
            # Calculate cutoff date
            cutoff_date = datetime.now(timezone.utc).timestamp() - (days_old * 24 * 60 * 60)
            cutoff_iso = datetime.fromtimestamp(cutoff_date, tz=timezone.utc).isoformat()
            
            # Delete old embeddings
            response = self.es_client.delete_by_query(
                index=index_name,
                body={
                    "query": {
                        "range": {
                            "created_at": {
                                "lt": cutoff_iso
                            }
                        }
                    }
                }
            )
            
            deleted_count = response.get('deleted', 0)
            self.logger.info(f"Cleaned up {deleted_count} old embeddings")
            return deleted_count
            
        except Exception as e:
            self.logger.error(f"Failed to cleanup old embeddings: {e}")
            return 0


class EmbeddingChangeDetector:
    """
    Detects when documents need re-embedding based on content changes.
    """
    
    def __init__(self, embedding_storage: CloudEmbeddingStorage, sync_cache: SyncCache):
        """
        Initialize change detector.
        
        Args:
            embedding_storage: Cloud embedding storage instance
            sync_cache: Sync cache for version tracking
        """
        self.embedding_storage = embedding_storage
        self.sync_cache = sync_cache
        self.logger = get_logger(__name__)
    
    def needs_embedding(self, content: str, source_id: str, version_info: VersionInfo) -> Tuple[bool, str]:
        """
        Check if content needs embedding/re-embedding.
        
        Args:
            content: Document content
            source_id: Source identifier
            version_info: Version information from sync system
            
        Returns:
            Tuple of (needs_embedding, reason)
        """
        # Compute content hash
        content_hash = self._compute_content_hash(content)
        
        # Check if embedding exists in cloud storage
        existing_embedding = self.embedding_storage.get_embedding(content_hash)
        
        if not existing_embedding:
            return True, "No existing embedding found"
        
        # Check if embedding is for the same content hash as current version
        if existing_embedding.content_hash != version_info.version_hash:
            return True, f"Content hash changed: {existing_embedding.content_hash[:8]} -> {version_info.version_hash[:8]}"
        
        # Check if embedding model has changed
        if existing_embedding.model != DEFAULT_EMBEDDING_MODEL:
            return True, f"Embedding model changed: {existing_embedding.model} -> {DEFAULT_EMBEDDING_MODEL}"
        
        # Check if embedding is too old (optional - could be configurable)
        embedding_age_days = (datetime.now(timezone.utc) - existing_embedding.created_at).days
        if embedding_age_days > 365:  # Re-embed after 1 year
            return True, f"Embedding is {embedding_age_days} days old"
        
        return False, "Embedding is up to date"
    
    def get_documents_needing_embedding(self, documents: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Filter documents that need embedding.
        
        Args:
            documents: List of documents with content and metadata
            
        Returns:
            List of documents that need embedding
        """
        documents_needing_embedding = []
        
        for doc in documents:
            content = doc.get('content', '')
            source_id = doc.get('source_id', '')
            version_info = doc.get('version_info')
            
            if not content or not version_info:
                self.logger.warning(f"Skipping document without content or version info: {source_id}")
                continue
            
            needs_embedding, reason = self.needs_embedding(content, source_id, version_info)
            
            if needs_embedding:
                self.logger.info(f"Document {source_id} needs embedding: {reason}")
                documents_needing_embedding.append(doc)
            else:
                self.logger.debug(f"Document {source_id} embedding is up to date: {reason}")
        
        return documents_needing_embedding
    
    def _compute_content_hash(self, content: str) -> str:
        """Compute SHA-256 hash of content."""
        return hashlib.sha256(content.strip().encode('utf-8')).hexdigest()


class BatchEmbeddingProcessor:
    """
    Handles batch processing for efficient embedding generation.
    """
    
    def __init__(self, openai_client: OpenAI, embedding_storage: CloudEmbeddingStorage,
                 batch_size: int = DEFAULT_BATCH_SIZE, max_workers: int = 3):
        """
        Initialize batch embedding processor.
        
        Args:
            openai_client: OpenAI client for embedding generation
            embedding_storage: Cloud embedding storage
            batch_size: Number of documents to process in each batch
            max_workers: Maximum number of worker threads
        """
        self.openai_client = openai_client
        self.embedding_storage = embedding_storage
        self.batch_size = batch_size
        self.max_workers = max_workers
        self.logger = get_logger(__name__)
    
    def process_documents(self, documents: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Process documents in batches to generate embeddings.
        
        Args:
            documents: List of documents to process
            
        Returns:
            Processing results summary
        """
        if not documents:
            return {
                'total_documents': 0,
                'processed_documents': 0,
                'failed_documents': 0,
                'batches_processed': 0,
                'errors': []
            }
        
        self.logger.info(f"Starting batch embedding processing for {len(documents)} documents")
        
        # Split documents into batches
        batches = self._create_batches(documents)
        
        results = {
            'total_documents': len(documents),
            'processed_documents': 0,
            'failed_documents': 0,
            'batches_processed': 0,
            'errors': []
        }
        
        # Process batches with thread pool
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_batch = {
                executor.submit(self._process_batch, batch): batch 
                for batch in batches
            }
            
            for future in as_completed(future_to_batch):
                batch = future_to_batch[future]
                try:
                    batch_result = future.result()
                    results['batches_processed'] += 1
                    results['processed_documents'] += batch_result['processed_count']
                    results['failed_documents'] += batch_result['failed_count']
                    
                    if batch_result['errors']:
                        results['errors'].extend(batch_result['errors'])
                    
                    self.logger.info(f"Batch {batch.batch_id} completed: "
                                   f"{batch_result['processed_count']} processed, "
                                   f"{batch_result['failed_count']} failed")
                    
                except Exception as e:
                    error_msg = f"Batch {batch.batch_id} failed: {e}"
                    self.logger.error(error_msg)
                    results['errors'].append(error_msg)
                    results['failed_documents'] += len(batch.documents)
        
        self.logger.info(f"Batch processing completed: {results}")
        return results
    
    def _create_batches(self, documents: List[Dict[str, Any]]) -> List[EmbeddingBatch]:
        """Create batches from documents."""
        batches = []
        
        for i in range(0, len(documents), self.batch_size):
            batch_docs = documents[i:i + self.batch_size]
            batch_id = f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{i // self.batch_size}"
            
            batch = EmbeddingBatch(
                batch_id=batch_id,
                documents=batch_docs,
                batch_size=len(batch_docs),
                created_at=datetime.now(timezone.utc)
            )
            batches.append(batch)
        
        return batches
    
    def _process_batch(self, batch: EmbeddingBatch) -> Dict[str, Any]:
        """Process a single batch of documents."""
        batch.status = 'processing'
        
        results = {
            'batch_id': batch.batch_id,
            'processed_count': 0,
            'failed_count': 0,
            'errors': []
        }
        
        try:
            # Extract content for embedding
            contents = []
            doc_metadata = []
            
            for doc in batch.documents:
                content = doc.get('content', '').strip()
                if content:
                    contents.append(content)
                    doc_metadata.append({
                        'source_id': doc.get('source_id', ''),
                        'version_info': doc.get('version_info'),
                        'document': doc
                    })
            
            if not contents:
                results['failed_count'] = len(batch.documents)
                results['errors'].append("No valid content found in batch")
                return results
            
            # Generate embeddings using OpenAI API
            try:
                response = self.openai_client.embeddings.create(
                    input=contents,
                    model=DEFAULT_EMBEDDING_MODEL
                )
                
                embeddings = response.data
                
                # Process each embedding
                embedding_infos = []
                for i, embedding in enumerate(embeddings):
                    try:
                        content = contents[i]
                        metadata = doc_metadata[i]
                        
                        content_hash = hashlib.sha256(content.encode('utf-8')).hexdigest()
                        
                        embedding_info = EmbeddingInfo(
                            content_hash=content_hash,
                            embedding_vector=embedding.embedding,
                            model=DEFAULT_EMBEDDING_MODEL,
                            created_at=datetime.now(timezone.utc),
                            source_id=metadata['source_id'],
                            content_size=len(content.encode('utf-8')),
                            metadata={
                                'version_info': metadata['version_info'].model_dump(mode='json') if metadata['version_info'] else {},
                                'content_preview': content[:200] + '...' if len(content) > 200 else content
                            }
                        )
                        
                        embedding_infos.append(embedding_info)
                        results['processed_count'] += 1
                        
                    except Exception as e:
                        error_msg = f"Failed to process embedding {i}: {e}"
                        self.logger.error(error_msg)
                        results['errors'].append(error_msg)
                        results['failed_count'] += 1
                
                # Store embeddings in batch
                if embedding_infos:
                    stored_count = self.embedding_storage.batch_store_embeddings(embedding_infos)
                    if stored_count != len(embedding_infos):
                        self.logger.warning(f"Only {stored_count}/{len(embedding_infos)} embeddings stored successfully")
                
            except Exception as e:
                error_msg = f"OpenAI API error in batch {batch.batch_id}: {e}"
                self.logger.error(error_msg)
                results['errors'].append(error_msg)
                results['failed_count'] = len(batch.documents)
            
            batch.status = 'completed'
            
        except Exception as e:
            batch.status = 'failed'
            error_msg = f"Batch processing error: {e}"
            self.logger.error(error_msg)
            results['errors'].append(error_msg)
            results['failed_count'] = len(batch.documents)
        
        return results


class EmbeddingCacheManager:
    """
    Manages download/upload of embedding cache for cloud-native operations.
    """
    
    def __init__(self, embedding_storage: CloudEmbeddingStorage, local_cache_path: str):
        """
        Initialize cache manager.
        
        Args:
            embedding_storage: Cloud embedding storage
            local_cache_path: Path to local cache file
        """
        self.embedding_storage = embedding_storage
        self.local_cache_path = Path(local_cache_path)
        self.logger = get_logger(__name__)
    
    def download_cache(self) -> Dict[str, Any]:
        """
        Download embedding cache from cloud storage to local cache.
        
        Returns:
            Download results summary
        """
        try:
            self.logger.info("Downloading embedding cache from cloud storage")
            
            # Query all embeddings from Elasticsearch
            index_name = f"{self.embedding_storage.index_prefix}_cache"
            
            # Use scroll API for large datasets
            search_body = {
                "query": {"match_all": {}},
                "size": 1000
            }
            
            response = self.embedding_storage.es_client.search(
                index=index_name,
                body=search_body,
                scroll='2m'
            )
            
            scroll_id = response['_scroll_id']
            embeddings_downloaded = 0
            
            # Prepare local cache data structure
            cache_data = {}
            
            while True:
                hits = response['hits']['hits']
                if not hits:
                    break
                
                for hit in hits:
                    source = hit['_source']
                    content_hash = source['content_hash']
                    
                    cache_data[content_hash] = {
                        'embedding_vector': source['embedding_vector'],
                        'model': source['model'],
                        'created_at': source['created_at'],
                        'metadata': source.get('metadata', {})
                    }
                    embeddings_downloaded += 1
                
                # Get next batch
                response = self.embedding_storage.es_client.scroll(
                    scroll_id=scroll_id,
                    scroll='2m'
                )
            
            # Clear scroll
            self.embedding_storage.es_client.clear_scroll(scroll_id=scroll_id)
            
            # Save to local cache file
            self.local_cache_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.local_cache_path, 'w', encoding='utf-8') as f:
                json.dump(cache_data, f, indent=2, ensure_ascii=False)
            
            self.logger.info(f"Downloaded {embeddings_downloaded} embeddings to local cache")
            
            return {
                'embeddings_downloaded': embeddings_downloaded,
                'cache_file': str(self.local_cache_path),
                'success': True
            }
            
        except Exception as e:
            error_msg = f"Failed to download embedding cache: {e}"
            self.logger.error(error_msg)
            return {
                'embeddings_downloaded': 0,
                'cache_file': str(self.local_cache_path),
                'success': False,
                'error': error_msg
            }
    
    def upload_cache(self) -> Dict[str, Any]:
        """
        Upload local embedding cache to cloud storage.
        
        Returns:
            Upload results summary
        """
        try:
            if not self.local_cache_path.exists():
                return {
                    'embeddings_uploaded': 0,
                    'success': False,
                    'error': 'Local cache file not found'
                }
            
            self.logger.info("Uploading embedding cache to cloud storage")
            
            # Load local cache data
            with open(self.local_cache_path, 'r', encoding='utf-8') as f:
                cache_data = json.load(f)
            
            # Convert to EmbeddingInfo objects
            embedding_infos = []
            for content_hash, data in cache_data.items():
                embedding_info = EmbeddingInfo(
                    content_hash=content_hash,
                    embedding_vector=data['embedding_vector'],
                    model=data['model'],
                    created_at=datetime.fromisoformat(data['created_at']),
                    source_id=data.get('metadata', {}).get('source_id', ''),
                    content_size=data.get('metadata', {}).get('content_size', 0),
                    metadata=data.get('metadata', {})
                )
                embedding_infos.append(embedding_info)
            
            # Upload in batches
            batch_size = 100
            total_uploaded = 0
            
            for i in range(0, len(embedding_infos), batch_size):
                batch = embedding_infos[i:i + batch_size]
                uploaded_count = self.embedding_storage.batch_store_embeddings(batch)
                total_uploaded += uploaded_count
                
                self.logger.info(f"Uploaded batch {i // batch_size + 1}: {uploaded_count} embeddings")
            
            self.logger.info(f"Uploaded {total_uploaded} embeddings to cloud storage")
            
            return {
                'embeddings_uploaded': total_uploaded,
                'success': True
            }
            
        except Exception as e:
            error_msg = f"Failed to upload embedding cache: {e}"
            self.logger.error(error_msg)
            return {
                'embeddings_uploaded': 0,
                'success': False,
                'error': error_msg
            }


class SyncEmbeddingProcessor:
    """
    Main orchestrator for embedding processing in the sync workflow.
    """
    
    def __init__(self, vector_store: VectorStoreES, openai_client: OpenAI, 
                 sync_cache: SyncCache, embedding_cache_path: str):
        """
        Initialize sync embedding processor.
        
        Args:
            vector_store: Vector store instance
            openai_client: OpenAI client
            sync_cache: Sync cache instance
            embedding_cache_path: Path to embedding cache file
        """
        self.vector_store = vector_store
        self.openai_client = openai_client
        self.sync_cache = sync_cache
        
        # Initialize components
        self.embedding_storage = CloudEmbeddingStorage(vector_store)
        self.change_detector = EmbeddingChangeDetector(self.embedding_storage, sync_cache)
        self.batch_processor = BatchEmbeddingProcessor(openai_client, self.embedding_storage)
        self.cache_manager = EmbeddingCacheManager(self.embedding_storage, embedding_cache_path)
        
        self.logger = get_logger(__name__)
    
    def process_sync_content(self, processed_content: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Process content from sync workflow for embedding generation.
        
        Args:
            processed_content: List of processed documents from sync workflow
            
        Returns:
            Processing results summary
        """
        self.logger.info(f"Processing {len(processed_content)} documents for embedding")
        
        # Filter documents that need embedding
        documents_needing_embedding = self.change_detector.get_documents_needing_embedding(processed_content)
        
        if not documents_needing_embedding:
            self.logger.info("No documents need embedding - all are up to date")
            return {
                'total_documents': len(processed_content),
                'documents_needing_embedding': 0,
                'processed_documents': 0,
                'embedding_results': {},
                'cache_operations': {}
            }
        
        self.logger.info(f"{len(documents_needing_embedding)} documents need embedding")
        
        # Process embeddings in batches
        embedding_results = self.batch_processor.process_documents(documents_needing_embedding)
        
        # Update sync cache with embedding status
        for doc in documents_needing_embedding:
            source_id = doc.get('source_id', '')
            if source_id:
                self.sync_cache.log_sync_operation(
                    source_id=source_id,
                    operation='embedding',
                    status='completed' if doc in processed_content else 'failed',
                    details={'embedding_model': DEFAULT_EMBEDDING_MODEL}
                )
        
        return {
            'total_documents': len(processed_content),
            'documents_needing_embedding': len(documents_needing_embedding),
            'processed_documents': embedding_results['processed_documents'],
            'embedding_results': embedding_results,
            'cache_operations': {}
        }
    
    def download_embedding_cache(self) -> Dict[str, Any]:
        """Download embedding cache from cloud."""
        return self.cache_manager.download_cache()
    
    def upload_embedding_cache(self) -> Dict[str, Any]:
        """Upload embedding cache to cloud."""
        return self.cache_manager.upload_cache()
    
    def get_embedding_statistics(self) -> Dict[str, Any]:
        """Get embedding storage statistics."""
        try:
            index_name = f"{self.embedding_storage.index_prefix}_cache"
            
            # Get total count
            count_response = self.embedding_storage.es_client.count(index=index_name)
            total_embeddings = count_response.get('count', 0)
            
            # Get storage size
            stats_response = self.embedding_storage.es_client.indices.stats(index=index_name)
            index_size_bytes = stats_response['indices'][index_name]['total']['store']['size_in_bytes']
            index_size_mb = index_size_bytes / (1024 * 1024)
            
            # Get model distribution
            agg_response = self.embedding_storage.es_client.search(
                index=index_name,
                body={
                    "size": 0,
                    "aggs": {
                        "models": {
                            "terms": {"field": "model"}
                        }
                    }
                }
            )
            
            model_distribution = {}
            for bucket in agg_response['aggregations']['models']['buckets']:
                model_distribution[bucket['key']] = bucket['doc_count']
            
            return {
                'total_embeddings': total_embeddings,
                'storage_size_mb': round(index_size_mb, 2),
                'model_distribution': model_distribution,
                'index_name': index_name
            }
            
        except Exception as e:
            self.logger.error(f"Failed to get embedding statistics: {e}")
            return {
                'total_embeddings': 0,
                'storage_size_mb': 0,
                'model_distribution': {},
                'error': str(e)
            }