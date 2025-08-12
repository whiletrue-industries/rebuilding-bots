"""
Tests for the embedding processor functionality.
"""

import pytest
import tempfile
import json
import numpy as np
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock

from ..embedding_processor import (
    CloudEmbeddingStorage, EmbeddingChangeDetector, BatchEmbeddingProcessor,
    EmbeddingCacheManager, SyncEmbeddingProcessor, EmbeddingInfo, EmbeddingBatch
)
from ..config import ContentSource, SourceType, VersionInfo, VersioningStrategy, FetchStrategy
from ..cache import SyncCache


class TestEmbeddingInfo:
    """Test the EmbeddingInfo dataclass."""
    
    def test_embedding_info_creation(self):
        """Test creating an EmbeddingInfo instance."""
        embedding_info = EmbeddingInfo(
            content_hash="abcd1234",
            embedding_vector=[0.1, 0.2, 0.3],
            model="text-embedding-3-small",
            created_at=datetime.now(timezone.utc),
            source_id="test-source",
            content_size=100,
            metadata={"test": "data"}
        )
        
        assert embedding_info.content_hash == "abcd1234"
        assert embedding_info.embedding_vector == [0.1, 0.2, 0.3]
        assert embedding_info.model == "text-embedding-3-small"
        assert embedding_info.source_id == "test-source"
        assert embedding_info.content_size == 100
        assert embedding_info.metadata == {"test": "data"}


class TestEmbeddingBatch:
    """Test the EmbeddingBatch dataclass."""
    
    def test_embedding_batch_creation(self):
        """Test creating an EmbeddingBatch instance."""
        documents = [
            {"content": "test content 1", "source_id": "source-1"},
            {"content": "test content 2", "source_id": "source-2"}
        ]
        
        batch = EmbeddingBatch(
            batch_id="batch-123",
            documents=documents,
            batch_size=2,
            created_at=datetime.now(timezone.utc)
        )
        
        assert batch.batch_id == "batch-123"
        assert len(batch.documents) == 2
        assert batch.batch_size == 2
        assert batch.status == "pending"


class TestCloudEmbeddingStorage:
    """Test the CloudEmbeddingStorage class."""
    
    @pytest.fixture
    def mock_vector_store(self):
        """Create a mock vector store."""
        mock_store = Mock()
        mock_store.es_client = Mock()
        return mock_store
    
    @pytest.fixture
    def embedding_storage(self, mock_vector_store):
        """Create an embedding storage instance."""
        with patch.object(CloudEmbeddingStorage, '_ensure_embedding_index_exists'):
            return CloudEmbeddingStorage(mock_vector_store)
    
    @pytest.fixture
    def sample_embedding_info(self):
        """Create a sample embedding info."""
        return EmbeddingInfo(
            content_hash="test-hash-123",
            embedding_vector=[0.1, 0.2, 0.3] * 512,  # 1536 dimensions
            model="text-embedding-3-small",
            created_at=datetime.now(timezone.utc),
            source_id="test-source",
            content_size=100,
            metadata={"test": "metadata"}
        )
    
    def test_store_embedding_success(self, embedding_storage, sample_embedding_info):
        """Test successful embedding storage."""
        embedding_storage.es_client.index.return_value = {"result": "created"}
        
        result = embedding_storage.store_embedding(sample_embedding_info)
        
        assert result is True
        embedding_storage.es_client.index.assert_called_once()
        
        # Check the call arguments
        call_args = embedding_storage.es_client.index.call_args
        assert call_args[1]['id'] == sample_embedding_info.content_hash
        assert call_args[1]['body']['content_hash'] == sample_embedding_info.content_hash
    
    def test_store_embedding_failure(self, embedding_storage, sample_embedding_info):
        """Test embedding storage failure."""
        embedding_storage.es_client.index.side_effect = Exception("ES error")
        
        result = embedding_storage.store_embedding(sample_embedding_info)
        
        assert result is False
    
    def test_get_embedding_found(self, embedding_storage):
        """Test retrieving an existing embedding."""
        mock_response = {
            'found': True,
            '_source': {
                'content_hash': 'test-hash',
                'embedding_vector': [0.1, 0.2, 0.3],
                'model': 'text-embedding-3-small',
                'created_at': '2024-01-01T00:00:00+00:00',
                'source_id': 'test-source',
                'content_size': 100,
                'metadata': {'test': 'data'}
            }
        }
        embedding_storage.es_client.get.return_value = mock_response
        
        result = embedding_storage.get_embedding('test-hash')
        
        assert result is not None
        assert result.content_hash == 'test-hash'
        assert result.embedding_vector == [0.1, 0.2, 0.3]
        assert result.model == 'text-embedding-3-small'
    
    def test_get_embedding_not_found(self, embedding_storage):
        """Test retrieving a non-existent embedding."""
        mock_response = {'found': False}
        embedding_storage.es_client.get.return_value = mock_response
        
        result = embedding_storage.get_embedding('nonexistent-hash')
        
        assert result is None
    
    @patch('botnim.sync.embedding_processor.bulk')
    def test_batch_store_embeddings(self, mock_bulk, embedding_storage):
        """Test batch storage of embeddings."""
        embeddings = [
            EmbeddingInfo(
                content_hash=f"hash-{i}",
                embedding_vector=[0.1] * 1536,
                model="text-embedding-3-small",
                created_at=datetime.now(timezone.utc),
                source_id=f"source-{i}",
                content_size=100,
                metadata={}
            )
            for i in range(3)
        ]
        mock_bulk.return_value = (3, [])
        result = embedding_storage.batch_store_embeddings(embeddings)
        assert result == 3
        mock_bulk.assert_called_once()


class TestEmbeddingChangeDetector:
    """Test the EmbeddingChangeDetector class."""
    
    @pytest.fixture
    def mock_embedding_storage(self):
        """Create a mock embedding storage."""
        return Mock(spec=CloudEmbeddingStorage)
    
    @pytest.fixture
    def mock_sync_cache(self):
        """Create a mock sync cache."""
        return Mock(spec=SyncCache)
    
    @pytest.fixture
    def change_detector(self, mock_embedding_storage, mock_sync_cache):
        """Create a change detector instance."""
        return EmbeddingChangeDetector(mock_embedding_storage, mock_sync_cache)
    
    @pytest.fixture
    def sample_version_info(self):
        """Create a sample version info."""
        return VersionInfo(
            source_id="test-source",
            version_hash="current-hash",
            version_timestamp=datetime.now(timezone.utc),
            content_size=100,
            last_fetch=datetime.now(timezone.utc),
            fetch_status="success"
        )
    
    def test_needs_embedding_no_existing(self, change_detector, sample_version_info):
        """Test when no existing embedding is found."""
        change_detector.embedding_storage.get_embedding.return_value = None
        
        needs_embedding, reason = change_detector.needs_embedding(
            "test content", "test-source", sample_version_info
        )
        
        assert needs_embedding is True
        assert "No existing embedding found" in reason
    
    def test_needs_embedding_hash_changed(self, change_detector, sample_version_info):
        """Test when content hash has changed."""
        old_embedding = EmbeddingInfo(
            content_hash="old-hash",
            embedding_vector=[0.1] * 1536,
            model="text-embedding-3-small",
            created_at=datetime.now(timezone.utc),
            source_id="test-source",
            content_size=100,
            metadata={}
        )
        change_detector.embedding_storage.get_embedding.return_value = old_embedding
        
        needs_embedding, reason = change_detector.needs_embedding(
            "test content", "test-source", sample_version_info
        )
        
        assert needs_embedding is True
        assert "Content hash changed" in reason
    
    def test_needs_embedding_model_changed(self, change_detector, sample_version_info):
        """Test when embedding model has changed."""
        # Create embedding with matching hash but different model
        content = "test content"
        content_hash = change_detector._compute_content_hash(content)
        sample_version_info.version_hash = content_hash
        
        old_embedding = EmbeddingInfo(
            content_hash=content_hash,
            embedding_vector=[0.1] * 1536,
            model="old-model",
            created_at=datetime.now(timezone.utc),
            source_id="test-source",
            content_size=100,
            metadata={}
        )
        change_detector.embedding_storage.get_embedding.return_value = old_embedding
        
        needs_embedding, reason = change_detector.needs_embedding(
            content, "test-source", sample_version_info
        )
        
        assert needs_embedding is True
        assert "Embedding model changed" in reason
    
    def test_needs_embedding_up_to_date(self, change_detector, sample_version_info):
        """Test when embedding is up to date."""
        content = "test content"
        content_hash = change_detector._compute_content_hash(content)
        sample_version_info.version_hash = content_hash
        
        current_embedding = EmbeddingInfo(
            content_hash=content_hash,
            embedding_vector=[0.1] * 1536,
            model="text-embedding-3-small",
            created_at=datetime.now(timezone.utc),
            source_id="test-source",
            content_size=100,
            metadata={}
        )
        change_detector.embedding_storage.get_embedding.return_value = current_embedding
        
        needs_embedding, reason = change_detector.needs_embedding(
            content, "test-source", sample_version_info
        )
        
        assert needs_embedding is False
        assert "up to date" in reason


class TestBatchEmbeddingProcessor:
    """Test the BatchEmbeddingProcessor class."""
    
    @pytest.fixture
    def mock_openai_client(self):
        """Create a mock OpenAI client."""
        client = Mock()
        # Mock the embeddings.create response
        mock_response = Mock()
        mock_response.data = [
            Mock(embedding=[0.1] * 1536),
            Mock(embedding=[0.2] * 1536)
        ]
        client.embeddings.create.return_value = mock_response
        return client
    
    @pytest.fixture
    def mock_embedding_storage(self):
        """Create a mock embedding storage."""
        storage = Mock(spec=CloudEmbeddingStorage)
        storage.batch_store_embeddings.return_value = 2  # Successfully stored 2 embeddings
        return storage
    
    @pytest.fixture
    def batch_processor(self, mock_openai_client, mock_embedding_storage):
        """Create a batch processor instance."""
        return BatchEmbeddingProcessor(
            openai_client=mock_openai_client,
            embedding_storage=mock_embedding_storage,
            batch_size=2,
            max_workers=1
        )
    
    @pytest.fixture
    def sample_documents(self):
        """Create sample documents for processing."""
        return [
            {
                "content": "This is test content 1",
                "source_id": "source-1",
                "version_info": VersionInfo(
                    source_id="source-1",
                    version_hash="hash-1",
                    version_timestamp=datetime.now(timezone.utc),
                    content_size=100,
                    last_fetch=datetime.now(timezone.utc),
                    fetch_status="success"
                )
            },
            {
                "content": "This is test content 2",
                "source_id": "source-2",
                "version_info": VersionInfo(
                    source_id="source-2",
                    version_hash="hash-2",
                    version_timestamp=datetime.now(timezone.utc),
                    content_size=100,
                    last_fetch=datetime.now(timezone.utc),
                    fetch_status="success"
                )
            }
        ]
    
    def test_process_documents_success(self, batch_processor, sample_documents):
        """Test successful document processing."""
        results = batch_processor.process_documents(sample_documents)
        
        assert results['total_documents'] == 2
        assert results['processed_documents'] == 2
        assert results['failed_documents'] == 0
        assert results['batches_processed'] == 1
        assert len(results['errors']) == 0
    
    def test_process_empty_documents(self, batch_processor):
        """Test processing empty document list."""
        results = batch_processor.process_documents([])
        
        assert results['total_documents'] == 0
        assert results['processed_documents'] == 0
        assert results['failed_documents'] == 0
        assert results['batches_processed'] == 0
    
    def test_create_batches(self, batch_processor, sample_documents):
        """Test batch creation."""
        # Add more documents to test batching
        documents = sample_documents + [
            {
                "content": "This is test content 3",
                "source_id": "source-3",
                "version_info": Mock()
            }
        ]
        
        batches = batch_processor._create_batches(documents)
        
        assert len(batches) == 2  # 3 documents, batch_size=2 -> 2 batches
        assert batches[0].batch_size == 2
        assert batches[1].batch_size == 1


class TestEmbeddingCacheManager:
    """Test the EmbeddingCacheManager class."""
    
    @pytest.fixture
    def temp_cache_file(self):
        """Create a temporary cache file."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            cache_data = {
                "hash1": {
                    "embedding_vector": [0.1, 0.2, 0.3],
                    "model": "text-embedding-3-small",
                    "created_at": "2024-01-01T00:00:00+00:00",
                    "metadata": {"source_id": "test"}
                }
            }
            json.dump(cache_data, f)
            f.flush()  # Ensure data is written to file
            yield f.name
        
        # Cleanup
        Path(f.name).unlink(missing_ok=True)
    
    @pytest.fixture
    def mock_embedding_storage(self):
        """Create a mock embedding storage."""
        storage = Mock(spec=CloudEmbeddingStorage)
        storage.index_prefix = "test_embeddings"
        return storage
    
    @pytest.fixture
    def cache_manager(self, mock_embedding_storage, temp_cache_file):
        """Create a cache manager instance."""
        return EmbeddingCacheManager(mock_embedding_storage, temp_cache_file)
    
    def test_upload_cache_success(self, cache_manager):
        """Test successful cache upload."""
        cache_manager.embedding_storage.batch_store_embeddings.return_value = 1
        
        results = cache_manager.upload_cache()
        
        assert results['success'] is True
        assert results['embeddings_uploaded'] == 1
    
    def test_upload_cache_file_not_found(self, mock_embedding_storage):
        """Test cache upload when file doesn't exist."""
        cache_manager = EmbeddingCacheManager(mock_embedding_storage, "nonexistent.json")
        
        results = cache_manager.upload_cache()
        
        assert results['success'] is False
        assert 'not found' in results['error']


class TestSyncEmbeddingProcessor:
    """Test the SyncEmbeddingProcessor class."""
    
    @pytest.fixture
    def mock_vector_store(self):
        """Create a mock vector store."""
        return Mock()
    
    @pytest.fixture
    def mock_openai_client(self):
        """Create a mock OpenAI client."""
        return Mock()
    
    @pytest.fixture
    def mock_sync_cache(self):
        """Create a mock sync cache."""
        return Mock(spec=SyncCache)
    
    @pytest.fixture
    def sync_processor(self, mock_vector_store, mock_openai_client, mock_sync_cache):
        """Create a sync embedding processor instance."""
        with patch('botnim.sync.embedding_processor.CloudEmbeddingStorage') as mock_storage_class, \
             patch('botnim.sync.embedding_processor.EmbeddingChangeDetector') as mock_detector_class, \
             patch('botnim.sync.embedding_processor.BatchEmbeddingProcessor') as mock_processor_class, \
             patch('botnim.sync.embedding_processor.EmbeddingCacheManager') as mock_manager_class:
            
            # Configure the mocks
            mock_storage_class.return_value = Mock()
            mock_detector_class.return_value = Mock()
            mock_processor_class.return_value = Mock()
            mock_manager_class.return_value = Mock()
            
            return SyncEmbeddingProcessor(
                vector_store=mock_vector_store,
                openai_client=mock_openai_client,
                sync_cache=mock_sync_cache,
                embedding_cache_path="./cache/embeddings.sqlite"
            )
    
    def test_process_sync_content_no_documents(self, sync_processor):
        """Test processing with no documents needing embedding."""
        sync_processor.change_detector.get_documents_needing_embedding.return_value = []
        
        results = sync_processor.process_sync_content([])
        
        assert results['total_documents'] == 0
        assert results['documents_needing_embedding'] == 0
        assert results['processed_documents'] == 0
    
    def test_process_sync_content_with_documents(self, sync_processor):
        """Test processing documents that need embedding."""
        processed_content = [
            {
                "content": "Test content",
                "source_id": "test-source",
                "version_info": Mock()
            }
        ]
        
        sync_processor.change_detector.get_documents_needing_embedding.return_value = processed_content
        sync_processor.batch_processor.process_documents.return_value = {
            'processed_documents': 1,
            'failed_documents': 0,
            'errors': []
        }
        
        results = sync_processor.process_sync_content(processed_content)
        
        assert results['total_documents'] == 1
        assert results['documents_needing_embedding'] == 1
        assert results['processed_documents'] == 1


class TestIntegration:
    """Integration tests for the embedding system."""
    
    @pytest.fixture
    def temp_cache_dir(self):
        """Create a temporary cache directory."""
        with tempfile.TemporaryDirectory() as temp_dir:
            yield temp_dir
    
    def test_embedding_workflow_integration(self, temp_cache_dir):
        """Test the complete embedding workflow integration."""
        # This would be a more comprehensive integration test
        # that tests the flow from content processing to embedding storage
        
        # Mock the external dependencies
        with patch('botnim.sync.embedding_processor.CloudEmbeddingStorage') as mock_storage_class:
            mock_storage = Mock()
            mock_storage_class.return_value = mock_storage
            
            # Test that the components can be instantiated together
            vector_store = Mock()
            openai_client = Mock()
            sync_cache = Mock()
            
            processor = SyncEmbeddingProcessor(
                vector_store=vector_store,
                openai_client=openai_client,
                sync_cache=sync_cache,
                embedding_cache_path=f"{temp_cache_dir}/embeddings.sqlite"
            )
            
            assert processor is not None
            assert processor.embedding_storage is not None
            assert processor.change_detector is not None
            assert processor.batch_processor is not None
            assert processor.cache_manager is not None