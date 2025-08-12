"""
Caching Layer and Duplicate Detection for Automated Sync System

This module provides:
1. Content caching to avoid redundant processing
2. Duplicate detection to skip already processed documents
3. Version tracking for incremental updates
4. Cache persistence and management
"""

import json
import hashlib
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any, Union
from dataclasses import dataclass

from ..config import get_logger
from .config import ContentSource


@dataclass
class CacheEntry:
    """Represents a cached content entry."""
    source_id: str
    content_hash: str
    content_size: int
    timestamp: datetime
    metadata: Dict[str, Any]
    processed: bool = False
    error_message: Optional[str] = None


@dataclass
class DuplicateInfo:
    """Information about duplicate detection."""
    is_duplicate: bool
    existing_hash: Optional[str] = None
    existing_timestamp: Optional[datetime] = None
    reason: Optional[str] = None


class SyncCache:
    """
    Main caching layer for the sync system.
    
    Provides:
    - Content hash-based duplicate detection
    - Cache persistence and retrieval
    - Version tracking integration
    - Cache statistics and cleanup
    """
    
    def __init__(self, cache_directory: str = "./cache", environment: str = "staging"):
        self.cache_directory = Path(cache_directory)
        self.environment = environment
        self.cache_directory.mkdir(parents=True, exist_ok=True)
        
        # Database paths
        self.content_cache_path = self.cache_directory / "content_cache.sqlite"
        self.duplicate_cache_path = self.cache_directory / "duplicate_cache.sqlite"
        self.sync_log_path = self.cache_directory / "sync_log.json"
        
        # Initialize databases
        self._init_content_cache()
        self._init_duplicate_cache()
        
        # Setup logging
        self.logger = get_logger(__name__)
    
    def _init_content_cache(self):
        """Initialize the content cache database."""
        with sqlite3.connect(self.content_cache_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS content_cache (
                    source_id TEXT PRIMARY KEY,
                    content_hash TEXT NOT NULL,
                    content_size INTEGER NOT NULL,
                    timestamp TEXT NOT NULL,
                    metadata TEXT NOT NULL,
                    processed BOOLEAN DEFAULT FALSE,
                    error_message TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
            """)
            
            # Create indexes for better performance
            conn.execute("CREATE INDEX IF NOT EXISTS idx_content_hash ON content_cache(content_hash)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_timestamp ON content_cache(timestamp)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_processed ON content_cache(processed)")
    
    def _init_duplicate_cache(self):
        """Initialize the duplicate detection cache database."""
        with sqlite3.connect(self.duplicate_cache_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS duplicate_cache (
                    content_hash TEXT PRIMARY KEY,
                    source_ids TEXT NOT NULL,  -- JSON array of source IDs
                    first_seen TEXT NOT NULL,
                    last_seen TEXT NOT NULL,
                    count INTEGER DEFAULT 1
                )
            """)
            
            # Create indexes
            conn.execute("CREATE INDEX IF NOT EXISTS idx_duplicate_hash ON duplicate_cache(content_hash)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_duplicate_count ON duplicate_cache(count)")
    
    def compute_content_hash(self, content: Union[str, bytes]) -> str:
        """Compute SHA-256 hash of content."""
        if isinstance(content, str):
            content = content.encode('utf-8')
        return hashlib.sha256(content).hexdigest()
    
    def is_duplicate(self, source_id: str, content_hash: str, content_size: int) -> DuplicateInfo:
        """
        Check if content is a duplicate.
        
        Args:
            source_id: Unique identifier for the source
            content_hash: SHA-256 hash of the content
            content_size: Size of content in bytes
            
        Returns:
            DuplicateInfo with duplicate detection results
        """
        # Check if we've seen this exact content before
        with sqlite3.connect(self.duplicate_cache_path) as conn:
            cursor = conn.execute(
                "SELECT source_ids, first_seen, last_seen, count FROM duplicate_cache WHERE content_hash = ?",
                (content_hash,)
            )
            result = cursor.fetchone()
            
            if result:
                source_ids = json.loads(result[0])
                first_seen = datetime.fromisoformat(result[1])
                last_seen = datetime.fromisoformat(result[2])
                count = result[3]
                
                # Update duplicate cache
                if source_id not in source_ids:
                    source_ids.append(source_id)
                    count += 1
                
                conn.execute(
                    "UPDATE duplicate_cache SET source_ids = ?, last_seen = ?, count = ? WHERE content_hash = ?",
                    (json.dumps(source_ids), datetime.now(timezone.utc).isoformat(), count, content_hash)
                )
                
                return DuplicateInfo(
                    is_duplicate=True,
                    existing_hash=content_hash,
                    existing_timestamp=first_seen,
                    reason=f"Content hash {content_hash} already processed by {len(source_ids)} sources"
                )
            
            # Not a duplicate - add to cache
            source_ids = [source_id]
            now = datetime.now(timezone.utc).isoformat()
            conn.execute(
                "INSERT INTO duplicate_cache (content_hash, source_ids, first_seen, last_seen, count) VALUES (?, ?, ?, ?, ?)",
                (content_hash, json.dumps(source_ids), now, now, 1)
            )
            
            return DuplicateInfo(is_duplicate=False)
    
    def get_cached_content(self, source_id: str) -> Optional[CacheEntry]:
        """Retrieve cached content for a source."""
        with sqlite3.connect(self.content_cache_path) as conn:
            cursor = conn.execute(
                "SELECT content_hash, content_size, timestamp, metadata, processed, error_message FROM content_cache WHERE source_id = ?",
                (source_id,)
            )
            result = cursor.fetchone()
            
            if result:
                return CacheEntry(
                    source_id=source_id,
                    content_hash=result[0],
                    content_size=result[1],
                    timestamp=datetime.fromisoformat(result[2]),
                    metadata=json.loads(result[3]),
                    processed=bool(result[4]),
                    error_message=result[5]
                )
        
        return None
    
    def get_all_cached_content(self) -> List[CacheEntry]:
        """Get all cached content entries."""
        entries = []
        with sqlite3.connect(self.content_cache_path) as conn:
            cursor = conn.execute(
                "SELECT source_id, content_hash, content_size, timestamp, metadata, processed, error_message FROM content_cache"
            )
            
            for row in cursor.fetchall():
                entry = CacheEntry(
                    source_id=row[0],
                    content_hash=row[1],
                    content_size=row[2],
                    timestamp=datetime.fromisoformat(row[3]),
                    metadata=json.loads(row[4]),
                    processed=bool(row[5]),
                    error_message=row[6]
                )
                entries.append(entry)
        
        return entries
    
    def cache_content(self, source_id: str, content_hash: str, content_size: int, 
                     metadata: Dict[str, Any], processed: bool = False, 
                     error_message: Optional[str] = None) -> None:
        """Cache content information."""
        now = datetime.now(timezone.utc).isoformat()
        
        with sqlite3.connect(self.content_cache_path) as conn:
            conn.execute("""
                INSERT OR REPLACE INTO content_cache 
                (source_id, content_hash, content_size, timestamp, metadata, processed, error_message, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                source_id, content_hash, content_size, now, json.dumps(metadata),
                processed, error_message, now, now
            ))
    
    def mark_processed(self, source_id: str, processed: bool = True, 
                      error_message: Optional[str] = None) -> None:
        """Mark a cached entry as processed."""
        now = datetime.now(timezone.utc).isoformat()
        
        with sqlite3.connect(self.content_cache_path) as conn:
            conn.execute(
                "UPDATE content_cache SET processed = ?, error_message = ?, updated_at = ? WHERE source_id = ?",
                (processed, error_message, now, source_id)
            )
    
    def should_process_source(self, source: ContentSource, content_hash: str, 
                            content_size: int) -> Tuple[bool, str]:
        """
        Determine if a source should be processed based on caching.
        
        Args:
            source: Content source configuration
            content_hash: Hash of the content
            content_size: Size of content in bytes
            
        Returns:
            Tuple of (should_process, reason)
        """
        # Check for duplicates
        duplicate_info = self.is_duplicate(source.id, content_hash, content_size)
        if duplicate_info.is_duplicate:
            return False, f"Duplicate content: {duplicate_info.reason}"
        
        # Check if already processed successfully
        cached = self.get_cached_content(source.id)
        if cached and cached.processed and cached.content_hash == content_hash:
            return False, f"Already processed successfully: {source.id}"
        
        return True, "Processing required: content changed or new"
    
    def get_cache_statistics(self) -> Dict[str, Any]:
        """Get cache statistics."""
        with sqlite3.connect(self.content_cache_path) as conn:
            total_sources = conn.execute("SELECT COUNT(*) FROM content_cache").fetchone()[0]
            processed_sources = conn.execute("SELECT COUNT(*) FROM content_cache WHERE processed = 1").fetchone()[0]
            error_sources = conn.execute("SELECT COUNT(*) FROM content_cache WHERE error_message IS NOT NULL").fetchone()[0]
        
        with sqlite3.connect(self.duplicate_cache_path) as conn:
            total_duplicates = conn.execute("SELECT COUNT(*) FROM duplicate_cache").fetchone()[0]
            high_duplicate_count = conn.execute("SELECT COUNT(*) FROM duplicate_cache WHERE count > 1").fetchone()[0]
        
        return {
            "total_sources": total_sources,
            "processed_sources": processed_sources,
            "error_sources": error_sources,
            "success_rate": (processed_sources / total_sources * 100) if total_sources > 0 else 0,
            "total_duplicates": total_duplicates,
            "high_duplicate_count": high_duplicate_count,
            "cache_size_mb": self._get_cache_size_mb()
        }
    
    def _get_cache_size_mb(self) -> float:
        """Get total cache size in MB."""
        total_size = 0
        for db_path in [self.content_cache_path, self.duplicate_cache_path]:
            if db_path.exists():
                total_size += db_path.stat().st_size
        return total_size / (1024 * 1024)
    
    def cleanup_old_entries(self, days_old: int = 30) -> int:
        """Clean up cache entries older than specified days."""
        cutoff_date = datetime.now(timezone.utc).timestamp() - (days_old * 24 * 60 * 60)
        cutoff_iso = datetime.fromtimestamp(cutoff_date, tz=timezone.utc).isoformat()
        
        with sqlite3.connect(self.content_cache_path) as conn:
            deleted = conn.execute(
                "DELETE FROM content_cache WHERE timestamp < ?",
                (cutoff_iso,)
            ).rowcount
        
        self.logger.info(f"Cleaned up {deleted} old cache entries")
        return deleted
    
    def log_sync_operation(self, source_id: str, operation: str, status: str, 
                          details: Optional[Dict[str, Any]] = None) -> None:
        """Log sync operations for monitoring."""
        log_entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "source_id": source_id,
            "operation": operation,
            "status": status,
            "details": details or {}
        }
        
        # Append to log file
        with open(self.sync_log_path, 'a', encoding='utf-8') as f:
            f.write(json.dumps(log_entry, ensure_ascii=False) + '\n')
    
    def get_sync_logs(self, source_id: Optional[str] = None, 
                     limit: int = 100) -> List[Dict[str, Any]]:
        """Retrieve sync operation logs."""
        logs = []
        
        if self.sync_log_path.exists():
            with open(self.sync_log_path, 'r', encoding='utf-8') as f:
                for line in f:
                    if line.strip():
                        log_entry = json.loads(line)
                        if source_id is None or log_entry.get('source_id') == source_id:
                            logs.append(log_entry)
        
        # Return most recent logs first
        logs.sort(key=lambda x: x['timestamp'], reverse=True)
        return logs[:limit]


class DuplicateDetector:
    """
    Advanced duplicate detection with content similarity analysis.
    """
    
    def __init__(self, cache: SyncCache):
        self.cache = cache
        self.logger = get_logger(__name__)
    
    def detect_similar_content(self, content: str, threshold: float = 0.9) -> List[Dict[str, Any]]:
        """
        Detect similar content using fuzzy matching.
        This is a placeholder for more sophisticated similarity detection.
        """
        # TODO: Implement fuzzy string matching or semantic similarity
        # For now, return empty list
        return []
    
    def get_duplicate_summary(self) -> Dict[str, Any]:
        """Get summary of duplicate detection results."""
        with sqlite3.connect(self.cache.duplicate_cache_path) as conn:
            # Get most common duplicates
            common_duplicates = conn.execute("""
                SELECT content_hash, count, source_ids 
                FROM duplicate_cache 
                WHERE count > 1 
                ORDER BY count DESC 
                LIMIT 10
            """).fetchall()
            
            # Get duplicate statistics
            total_duplicates = conn.execute("SELECT COUNT(*) FROM duplicate_cache WHERE count > 1").fetchone()[0]
            total_saved = conn.execute("SELECT SUM(count - 1) FROM duplicate_cache WHERE count > 1").fetchone()[0] or 0
        
        return {
            "total_duplicates": total_duplicates,
            "total_processing_saved": total_saved,
            "most_common_duplicates": [
                {
                    "hash": row[0][:16] + "...",  # Truncate for display
                    "count": row[1],
                    "source_count": len(json.loads(row[2]))
                }
                for row in common_duplicates
            ]
        } 