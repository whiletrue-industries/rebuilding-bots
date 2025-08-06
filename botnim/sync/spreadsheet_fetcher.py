"""
Asynchronous Spreadsheet Fetcher for Automated Sync System

This module provides:
1. Async fetching of Google Sheets data
2. Background task processing for spreadsheet operations
3. Intermediate storage in Elasticsearch
4. Integration with existing sync workflow
"""

import asyncio
import aiohttp
import json
import hashlib
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any, Tuple
from pathlib import Path
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, Future
import threading
from dataclasses import dataclass

from ..config import get_logger
from .config import ContentSource, SpreadsheetSourceConfig, VersionInfo
from .cache import SyncCache
from ..vector_store.vector_store_es import VectorStoreES
from ..document_parser.pdf_processor.google_sheets_service import GoogleSheetsService

logger = get_logger(__name__)


@dataclass
class SpreadsheetData:
    """Represents fetched spreadsheet data."""
    source_id: str
    data: List[Dict[str, Any]]
    headers: List[str]
    row_count: int
    content_hash: str
    fetch_timestamp: datetime
    metadata: Dict[str, Any]


@dataclass
class ProcessingTask:
    """Represents a spreadsheet processing task."""
    task_id: str
    source_id: str
    status: str  # 'pending', 'processing', 'completed', 'failed'
    created_at: datetime
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    error_message: Optional[str] = None
    result: Optional[Dict[str, Any]] = None


class SpreadsheetFetcher:
    """
    Asynchronous fetcher for Google Sheets data.
    """
    
    def __init__(self, cache: SyncCache, vector_store: VectorStoreES):
        """
        Initialize spreadsheet fetcher.
        
        Args:
            cache: Sync cache for tracking
            vector_store: Vector store for intermediate storage
        """
        self.cache = cache
        self.vector_store = vector_store
        self.session = None
        self.logger = get_logger(__name__)
    
    async def __aenter__(self):
        """Async context manager entry."""
        self.session = aiohttp.ClientSession()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        if self.session:
            await self.session.close()
    
    async def fetch_spreadsheet_data(self, source: ContentSource) -> Optional[SpreadsheetData]:
        """
        Fetch spreadsheet data asynchronously.
        
        Args:
            source: Spreadsheet source configuration
            
        Returns:
            SpreadsheetData if successful, None otherwise
        """
        if not source.spreadsheet_config:
            self.logger.error(f"Spreadsheet source {source.id} missing spreadsheet_config")
            return None
        
        try:
            self.logger.info(f"Fetching spreadsheet data from {source.spreadsheet_config.url}")
            
            # Use existing GoogleSheetsService for fetching
            sheets_service = GoogleSheetsService(
                credentials_path=source.spreadsheet_config.credentials_path,
                use_adc=source.spreadsheet_config.use_adc
            )
            
            # Extract spreadsheet ID and sheet name from URL
            spreadsheet_id, sheet_name = self._parse_google_sheets_url(
                source.spreadsheet_config.url
            )
            
            if not spreadsheet_id or not sheet_name:
                self.logger.error(f"Invalid Google Sheets URL: {source.spreadsheet_config.url}")
                return None
            
            # Fetch data using the existing service
            data = await self._fetch_sheet_data_async(
                sheets_service, spreadsheet_id, sheet_name, source.spreadsheet_config.range
            )
            
            if not data:
                self.logger.warning(f"No data fetched from spreadsheet {source.id}")
                return None
            
            # Convert to structured format
            headers = data[0] if data else []
            rows = data[1:] if len(data) > 1 else []
            
            # Convert to list of dictionaries
            structured_data = []
            for row in rows:
                row_dict = {}
                for i, value in enumerate(row):
                    if i < len(headers):
                        row_dict[headers[i]] = value
                    else:
                        row_dict[f"column_{i+1}"] = value
                structured_data.append(row_dict)
            
            # Compute content hash
            content_str = json.dumps(structured_data, sort_keys=True, ensure_ascii=False)
            content_hash = hashlib.sha256(content_str.encode('utf-8')).hexdigest()
            
            # Create spreadsheet data object
            spreadsheet_data = SpreadsheetData(
                source_id=source.id,
                data=structured_data,
                headers=headers,
                row_count=len(structured_data),
                content_hash=content_hash,
                fetch_timestamp=datetime.now(timezone.utc),
                metadata={
                    'spreadsheet_id': spreadsheet_id,
                    'sheet_name': sheet_name,
                    'range': source.spreadsheet_config.range,
                    'url': source.spreadsheet_config.url
                }
            )
            
            self.logger.info(f"Successfully fetched {len(structured_data)} rows from {source.id}")
            return spreadsheet_data
            
        except Exception as e:
            self.logger.error(f"Failed to fetch spreadsheet data from {source.id}: {e}")
            return None
    
    async def _fetch_sheet_data_async(self, sheets_service: GoogleSheetsService, 
                                    spreadsheet_id: str, sheet_name: str, 
                                    range_str: Optional[str]) -> Optional[List[List]]:
        """
        Fetch sheet data asynchronously using thread pool.
        
        Args:
            sheets_service: Google Sheets service
            spreadsheet_id: Spreadsheet ID
            sheet_name: Sheet name
            range_str: Cell range (e.g., 'A1:Z1000')
            
        Returns:
            List of rows if successful, None otherwise
        """
        try:
            # Use thread pool to run synchronous Google Sheets API call
            loop = asyncio.get_event_loop()
            with ThreadPoolExecutor() as executor:
                # Create the range string
                if range_str:
                    full_range = f"{sheet_name}!{range_str}"
                else:
                    full_range = sheet_name
                
                # Fetch data in thread pool
                data = await loop.run_in_executor(
                    executor,
                    self._fetch_sheet_data_sync,
                    sheets_service,
                    spreadsheet_id,
                    full_range
                )
                
                return data
                
        except Exception as e:
            self.logger.error(f"Failed to fetch sheet data: {e}")
            return None
    
    def _fetch_sheet_data_sync(self, sheets_service: GoogleSheetsService, 
                              spreadsheet_id: str, range_str: str) -> Optional[List[List]]:
        """
        Synchronous method to fetch sheet data.
        
        Args:
            sheets_service: Google Sheets service
            spreadsheet_id: Spreadsheet ID
            range_str: Full range string (e.g., 'Sheet1!A1:Z1000')
            
        Returns:
            List of rows if successful, None otherwise
        """
        try:
            # Use the existing Google Sheets service to fetch data
            # This is a synchronous operation that we'll run in a thread pool
            service = sheets_service.sync.service
            
            # Fetch the data
            result = service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range=range_str
            ).execute()
            
            values = result.get('values', [])
            return values
            
        except Exception as e:
            self.logger.error(f"Failed to fetch sheet data synchronously: {e}")
            return None
    
    def _parse_google_sheets_url(self, url: str) -> Tuple[Optional[str], Optional[str]]:
        """
        Parse Google Sheets URL to extract spreadsheet ID and sheet name.
        
        Args:
            url: Google Sheets URL
            
        Returns:
            Tuple of (spreadsheet_id, sheet_name)
        """
        try:
            # Handle different URL formats
            if '/spreadsheets/d/' in url:
                # Extract spreadsheet ID
                start = url.find('/spreadsheets/d/') + 16
                end = url.find('/', start)
                if end == -1:
                    end = url.find('?', start)
                if end == -1:
                    end = len(url)
                
                spreadsheet_id = url[start:end]
                
                # Extract sheet name from gid parameter
                sheet_name = None
                if 'gid=' in url:
                    gid_start = url.find('gid=') + 4
                    gid_end = url.find('&', gid_start)
                    if gid_end == -1:
                        gid_end = len(url)
                    gid = url[gid_start:gid_end]
                    
                    # For now, we'll use a default sheet name
                    # In a full implementation, we'd map gid to sheet name
                    sheet_name = "Sheet1"  # Default sheet name
                
                return spreadsheet_id, sheet_name
            else:
                self.logger.error(f"Unsupported Google Sheets URL format: {url}")
                return None, None
                
        except Exception as e:
            self.logger.error(f"Failed to parse Google Sheets URL: {e}")
            return None, None


class TaskQueue:
    """
    Thread-based task queue for background spreadsheet processing.
    """
    
    def __init__(self, max_workers: int = 3):
        """
        Initialize task queue.
        
        Args:
            max_workers: Maximum number of worker threads
        """
        self.max_workers = max_workers
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.tasks: Dict[str, ProcessingTask] = {}
        self.task_lock = threading.Lock()
        self.logger = get_logger(__name__)
    
    def submit_task(self, task_id: str, source_id: str, func, *args, **kwargs) -> str:
        """
        Submit a task for background processing.
        
        Args:
            task_id: Unique task identifier
            source_id: Source identifier
            func: Function to execute
            *args: Function arguments
            **kwargs: Function keyword arguments
            
        Returns:
            Task ID
        """
        # Create task record
        task = ProcessingTask(
            task_id=task_id,
            source_id=source_id,
            status='pending',
            created_at=datetime.now(timezone.utc)
        )
        
        with self.task_lock:
            self.tasks[task_id] = task
        
        # Submit to thread pool
        future = self.executor.submit(self._execute_task, task_id, func, *args, **kwargs)
        
        self.logger.info(f"Submitted task {task_id} for source {source_id}")
        return task_id
    
    def _execute_task(self, task_id: str, func, *args, **kwargs):
        """
        Execute a task and update its status.
        
        Args:
            task_id: Task identifier
            func: Function to execute
            *args: Function arguments
            **kwargs: Function keyword arguments
        """
        with self.task_lock:
            if task_id not in self.tasks:
                return
            
            task = self.tasks[task_id]
            task.status = 'processing'
            task.started_at = datetime.now(timezone.utc)
        
        try:
            # Execute the function
            result = func(*args, **kwargs)
            
            # Update task status
            with self.task_lock:
                task.status = 'completed'
                task.completed_at = datetime.now(timezone.utc)
                task.result = result
            
            self.logger.info(f"Task {task_id} completed successfully")
            
        except Exception as e:
            # Update task status with error
            with self.task_lock:
                task.status = 'failed'
                task.completed_at = datetime.now(timezone.utc)
                task.error_message = str(e)
            
            self.logger.error(f"Task {task_id} failed: {e}")
    
    def get_task_status(self, task_id: str) -> Optional[ProcessingTask]:
        """
        Get task status.
        
        Args:
            task_id: Task identifier
            
        Returns:
            ProcessingTask if found, None otherwise
        """
        with self.task_lock:
            return self.tasks.get(task_id)
    
    def get_pending_tasks(self) -> List[ProcessingTask]:
        """
        Get all pending tasks.
        
        Returns:
            List of pending tasks
        """
        with self.task_lock:
            return [task for task in self.tasks.values() if task.status == 'pending']
    
    def get_processing_tasks(self) -> List[ProcessingTask]:
        """
        Get all processing tasks.
        
        Returns:
            List of processing tasks
        """
        with self.task_lock:
            return [task for task in self.tasks.values() if task.status == 'processing']
    
    def cleanup_completed_tasks(self, max_age_hours: int = 24) -> int:
        """
        Clean up completed tasks older than specified age.
        
        Args:
            max_age_hours: Maximum age in hours
            
        Returns:
            Number of tasks cleaned up
        """
        cutoff_time = datetime.now(timezone.utc) - timedelta(hours=max_age_hours)
        
        with self.task_lock:
            to_remove = []
            for task_id, task in self.tasks.items():
                if (task.status in ['completed', 'failed'] and 
                    task.completed_at and task.completed_at < cutoff_time):
                    to_remove.append(task_id)
            
            for task_id in to_remove:
                del self.tasks[task_id]
        
        self.logger.info(f"Cleaned up {len(to_remove)} completed tasks")
        return len(to_remove)
    
    def shutdown(self):
        """Shutdown the task queue."""
        self.executor.shutdown(wait=True)
        self.logger.info("Task queue shutdown complete")


class IntermediateStorage:
    """
    Elasticsearch-based intermediate storage for spreadsheet data.
    """
    
    def __init__(self, vector_store: VectorStoreES):
        """
        Initialize intermediate storage.
        
        Args:
            vector_store: Vector store for storage
        """
        self.vector_store = vector_store
        self.logger = get_logger(__name__)
    
    def store_spreadsheet_data(self, data: SpreadsheetData) -> bool:
        """
        Store spreadsheet data in Elasticsearch.
        
        Args:
            data: Spreadsheet data to store
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Create document for storage
            document = {
                'content': json.dumps(data.data, ensure_ascii=False),
                'metadata': {
                    'source_id': data.source_id,
                    'source_type': 'spreadsheet',
                    'headers': data.headers,
                    'row_count': data.row_count,
                    'content_hash': data.content_hash,
                    'fetch_timestamp': data.fetch_timestamp.isoformat(),
                    'spreadsheet_metadata': data.metadata,
                    'processing_status': 'intermediate',
                    'version': '1.0'
                },
                'vectors': []  # Will be populated during main sync
            }
            
            # Generate document ID
            doc_id = f"spreadsheet_{data.source_id}_{data.content_hash[:8]}"
            
            # Store in Elasticsearch
            # Note: We'll use a temporary index for intermediate storage
            index_name = f"spreadsheet_intermediate_{data.source_id}"
            
            # Create index if it doesn't exist
            self._ensure_index_exists(index_name)
            
            # Store document
            result = self.vector_store.es_client.index(
                index=index_name,
                id=doc_id,
                body=document
            )
            
            self.logger.info(f"Stored spreadsheet data for {data.source_id} with {data.row_count} rows")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to store spreadsheet data: {e}")
            return False
    
    def get_spreadsheet_data(self, source_id: str, content_hash: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        Retrieve spreadsheet data from Elasticsearch.
        
        Args:
            source_id: Source identifier
            content_hash: Optional content hash filter
            
        Returns:
            Document data if found, None otherwise
        """
        try:
            index_name = f"spreadsheet_intermediate_{source_id}"
            
            # Build query
            query = {
                "query": {
                    "bool": {
                        "must": [
                            {"term": {"metadata.source_id": source_id}},
                            {"term": {"metadata.source_type": "spreadsheet"}}
                        ]
                    }
                },
                "sort": [{"metadata.fetch_timestamp": {"order": "desc"}}],
                "size": 1
            }
            
            if content_hash:
                query["query"]["bool"]["must"].append(
                    {"term": {"metadata.content_hash": content_hash}}
                )
            
            # Search for document
            result = self.vector_store.es_client.search(
                index=index_name,
                body=query
            )
            
            hits = result.get('hits', {}).get('hits', [])
            if hits:
                return hits[0]['_source']
            
            return None
            
        except Exception as e:
            self.logger.error(f"Failed to retrieve spreadsheet data: {e}")
            return None
    
    def _ensure_index_exists(self, index_name: str):
        """Ensure Elasticsearch index exists."""
        try:
            if not self.vector_store.es_client.indices.exists(index=index_name):
                # Create index with basic mapping
                mapping = {
                    "mappings": {
                        "properties": {
                            "content": {"type": "text"},
                            "metadata": {
                                "type": "object",
                                "properties": {
                                    "source_id": {"type": "keyword"},
                                    "source_type": {"type": "keyword"},
                                    "content_hash": {"type": "keyword"},
                                    "fetch_timestamp": {"type": "date"},
                                    "processing_status": {"type": "keyword"}
                                }
                            }
                        }
                    }
                }
                
                self.vector_store.es_client.indices.create(
                    index=index_name,
                    body=mapping
                )
                
                self.logger.info(f"Created index: {index_name}")
                
        except Exception as e:
            self.logger.error(f"Failed to ensure index exists: {e}")


class AsyncSpreadsheetProcessor:
    """
    Main orchestrator for asynchronous spreadsheet processing.
    """
    
    def __init__(self, cache: SyncCache, vector_store: VectorStoreES, 
                 max_workers: int = 3):
        """
        Initialize async spreadsheet processor.
        
        Args:
            cache: Sync cache
            vector_store: Vector store for intermediate storage
            max_workers: Maximum number of worker threads
        """
        self.cache = cache
        self.vector_store = vector_store
        self.task_queue = TaskQueue(max_workers=max_workers)
        self.storage = IntermediateStorage(vector_store)
        self.logger = get_logger(__name__)
    
    async def process_spreadsheet_source(self, source: ContentSource) -> Dict[str, Any]:
        """
        Process a spreadsheet source asynchronously.
        
        Args:
            source: Spreadsheet source configuration
            
        Returns:
            Processing results summary
        """
        self.logger.info(f"Processing spreadsheet source: {source.id}")
        
        results = {
            'source_id': source.id,
            'task_id': None,
            'status': 'pending',
            'data_fetched': False,
            'data_stored': False,
            'error_message': None
        }
        
        try:
            # Check if source should be processed
            should_process, reason = self._should_process_source(source)
            if not should_process:
                results['status'] = 'skipped'
                results['error_message'] = reason
                self.logger.info(f"Skipping spreadsheet source {source.id}: {reason}")
                return results
            
            # Generate task ID
            task_id = f"spreadsheet_{source.id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            results['task_id'] = task_id
            
            # Submit task for background processing
            self.task_queue.submit_task(
                task_id=task_id,
                source_id=source.id,
                func=self._process_spreadsheet_task,
                source=source
            )
            
            results['status'] = 'submitted'
            self.logger.info(f"Submitted spreadsheet processing task: {task_id}")
            
            return results
            
        except Exception as e:
            results['status'] = 'error'
            results['error_message'] = str(e)
            self.logger.error(f"Failed to process spreadsheet source {source.id}: {e}")
            return results
    
    def _process_spreadsheet_task(self, source: ContentSource) -> Dict[str, Any]:
        """
        Process spreadsheet task in background thread.
        
        Args:
            source: Spreadsheet source configuration
            
        Returns:
            Processing results
        """
        results = {
            'source_id': source.id,
            'data_fetched': False,
            'data_stored': False,
            'row_count': 0,
            'content_hash': None,
            'error_message': None
        }
        
        try:
            # Fetch spreadsheet data
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            async def fetch_data():
                async with SpreadsheetFetcher(self.cache, self.vector_store) as fetcher:
                    return await fetcher.fetch_spreadsheet_data(source)
            
            spreadsheet_data = loop.run_until_complete(fetch_data())
            loop.close()
            
            if not spreadsheet_data:
                results['error_message'] = "Failed to fetch spreadsheet data"
                return results
            
            results['data_fetched'] = True
            results['row_count'] = spreadsheet_data.row_count
            results['content_hash'] = spreadsheet_data.content_hash
            
            # Store data in intermediate storage
            stored = self.storage.store_spreadsheet_data(spreadsheet_data)
            results['data_stored'] = stored
            
            if stored:
                # Update cache
                self.cache.cache_content(
                    source_id=source.id,
                    content_hash=spreadsheet_data.content_hash,
                    content_size=len(json.dumps(spreadsheet_data.data)),
                    metadata={
                        'row_count': spreadsheet_data.row_count,
                        'headers': spreadsheet_data.headers,
                        'fetch_timestamp': spreadsheet_data.fetch_timestamp.isoformat()
                    },
                    processed=True
                )
                
                self.logger.info(f"Successfully processed spreadsheet {source.id}: {spreadsheet_data.row_count} rows")
            else:
                results['error_message'] = "Failed to store spreadsheet data"
            
            return results
            
        except Exception as e:
            results['error_message'] = str(e)
            self.logger.error(f"Failed to process spreadsheet task for {source.id}: {e}")
            return results
    
    def _should_process_source(self, source: ContentSource) -> Tuple[bool, Optional[str]]:
        """
        Check if source should be processed.
        
        Args:
            source: Source configuration
            
        Returns:
            Tuple of (should_process, reason)
        """
        # Check if source is enabled
        if not source.enabled:
            return False, "Source is disabled"
        
        # Check if source has spreadsheet config
        if not source.spreadsheet_config:
            return False, "Missing spreadsheet configuration"
        
        # Check if source uses async strategy
        if source.fetch_strategy != "async":
            return False, "Source is not configured for async processing"
        
        return True, None
    
    def get_task_status(self, task_id: str) -> Optional[ProcessingTask]:
        """
        Get task status.
        
        Args:
            task_id: Task identifier
            
        Returns:
            ProcessingTask if found, None otherwise
        """
        return self.task_queue.get_task_status(task_id)
    
    def get_pending_tasks(self) -> List[ProcessingTask]:
        """Get all pending tasks."""
        return self.task_queue.get_pending_tasks()
    
    def get_processing_tasks(self) -> List[ProcessingTask]:
        """Get all processing tasks."""
        return self.task_queue.get_processing_tasks()
    
    def cleanup_completed_tasks(self, max_age_hours: int = 24) -> int:
        """Clean up completed tasks."""
        return self.task_queue.cleanup_completed_tasks(max_age_hours)
    
    def shutdown(self):
        """Shutdown the processor."""
        self.task_queue.shutdown()
        self.logger.info("Async spreadsheet processor shutdown complete")


# Convenience functions for integration with existing sync workflow

async def process_spreadsheet_source_async(source: ContentSource, cache: SyncCache, 
                                         vector_store: VectorStoreES) -> Dict[str, Any]:
    """
    Process a spreadsheet source asynchronously.
    
    Args:
        source: Spreadsheet source configuration
        cache: Sync cache
        vector_store: Vector store
        
    Returns:
        Processing results
    """
    processor = AsyncSpreadsheetProcessor(cache, vector_store)
    try:
        return await processor.process_spreadsheet_source(source)
    finally:
        processor.shutdown()


def get_spreadsheet_data_from_storage(source_id: str, vector_store: VectorStoreES, 
                                    content_hash: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """
    Get spreadsheet data from intermediate storage.
    
    Args:
        source_id: Source identifier
        vector_store: Vector store
        content_hash: Optional content hash filter
        
    Returns:
        Document data if found, None otherwise
    """
    storage = IntermediateStorage(vector_store)
    return storage.get_spreadsheet_data(source_id, content_hash) 