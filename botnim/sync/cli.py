"""
CLI commands for managing the sync cache and duplicate detection.
"""

import argparse
import json
from pathlib import Path
from typing import Optional

from ..config import get_logger
from .cache import SyncCache, DuplicateDetector
from .config import SyncConfig, VersionManager
from .pdf_discovery import process_pdf_source


def cache_stats_command(args):
    """Display cache statistics."""
    logger = get_logger("cache_cli")
    cache = SyncCache(cache_directory=args.cache_dir)
    stats = cache.get_cache_statistics()
    
    logger.info("📊 Cache Statistics")
    logger.info("=" * 50)
    logger.info(f"Total Sources: {stats['total_sources']}")
    logger.info(f"Processed Sources: {stats['processed_sources']}")
    logger.info(f"Error Sources: {stats['error_sources']}")
    logger.info(f"Success Rate: {stats['success_rate']:.1f}%")
    logger.info(f"Total Duplicates: {stats['total_duplicates']}")
    logger.info(f"High Duplicate Count: {stats['high_duplicate_count']}")
    logger.info(f"Cache Size: {stats['cache_size_mb']:.2f} MB")
    
    # Show recent sync logs
    logs = cache.get_sync_logs(limit=5)
    if logs:
        logger.info("\n📝 Recent Sync Operations")
        logger.info("-" * 30)
        for log in logs:
            logger.info(f"{log['timestamp'][:19]} | {log['source_id']} | {log['operation']} | {log['status']}")


def duplicate_summary_command(args):
    """Display duplicate detection summary."""
    logger = get_logger("cache_cli")
    cache = SyncCache(cache_directory=args.cache_dir)
    detector = DuplicateDetector(cache)
    summary = detector.get_duplicate_summary()
    
    logger.info("🔍 Duplicate Detection Summary")
    logger.info("=" * 40)
    logger.info(f"Total Duplicates: {summary['total_duplicates']}")
    logger.info(f"Processing Operations Saved: {summary['total_processing_saved']}")
    
    if summary['most_common_duplicates']:
        logger.info("\n📋 Most Common Duplicates")
        logger.info("-" * 30)
        for dup in summary['most_common_duplicates']:
            logger.info(f"Hash: {dup['hash']} | Count: {dup['count']} | Sources: {dup['source_count']}")


def cache_cleanup_command(args):
    """Clean up old cache entries."""
    logger = get_logger("cache_cli")
    cache = SyncCache(cache_directory=args.cache_dir)
    
    logger.info(f"🧹 Cleaning up cache entries older than {args.days} days...")
    deleted = cache.cleanup_old_entries(days_old=args.days)
    logger.info(f"Deleted {deleted} old entries")
    
    # Show updated statistics
    stats = cache.get_cache_statistics()
    logger.info(f"Remaining sources: {stats['total_sources']}")


def cache_logs_command(args):
    """Display sync operation logs."""
    logger = get_logger("cache_cli")
    cache = SyncCache(cache_directory=args.cache_dir)
    
    if args.source_id:
        logs = cache.get_sync_logs(source_id=args.source_id, limit=args.limit)
        logger.info(f"📝 Sync Logs for Source: {args.source_id}")
    else:
        logs = cache.get_sync_logs(limit=args.limit)
        logger.info("📝 Recent Sync Logs")
    
    logger.info("=" * 50)
    
    if not logs:
        logger.info("No logs found.")
        return
    
    for log in logs:
        timestamp = log['timestamp'][:19]  # Remove timezone info for display
        source_id = log['source_id']
        operation = log['operation']
        status = log['status']
        details = log.get('details', {})
        
        logger.info(f"{timestamp} | {source_id} | {operation} | {status}")
        if details:
            logger.info(f"  Details: {json.dumps(details, indent=2)}")


def pdf_discover_command(args):
    """Discover and process PDFs from a remote source."""
    logger = get_logger("pdf_cli")
    
    try:
        # Load sync configuration
        config = SyncConfig.from_yaml(args.config_file)
        source = config.get_source_by_id(args.source_id)
        
        if not source:
            logger.error(f"Source '{args.source_id}' not found in configuration")
            return
        
        if source.type.value != 'pdf':
            logger.error(f"Source '{args.source_id}' is not a PDF source")
            return
        
        logger.info(f"🔍 Discovering PDFs from source: {source.name}")
        
        # Initialize components
        cache = SyncCache(cache_directory=args.cache_dir)
        
        # Initialize vector store
        from botnim.vector_store.vector_store_es import VectorStoreES
        vector_store = VectorStoreES('', '.', environment=args.environment)
        
        # Initialize OpenAI client
        from botnim.cli import get_openai_client
        openai_client = get_openai_client()
        
        # Process the PDF source
        results = process_pdf_source(
            source=source,
            cache=cache,
            vector_store=vector_store,
            openai_client=openai_client,
            temp_directory=args.temp_dir
        )
        
        # Display results
        logger.info("📊 PDF Discovery Results")
        logger.info("=" * 40)
        logger.info(f"Source: {results['source_id']}")
        logger.info(f"Discovered PDFs: {results['discovered_pdfs']}")
        logger.info(f"Processed PDFs: {results['processed_pdfs']}")
        logger.info(f"Failed PDFs: {results['failed_pdfs']}")
        
        if results['errors']:
            logger.info("\n❌ Errors:")
            for error in results['errors']:
                logger.info(f"  - {error}")
        
        logger.info("✅ PDF discovery completed")
        
    except Exception as e:
        logger.error(f"PDF discovery failed: {e}")


def pdf_status_command(args):
    """Check PDF processing status."""
    logger = get_logger("pdf_cli")
    
    try:
        # Initialize vector store
        from botnim.vector_store.vector_store_es import VectorStoreES
        vector_store = VectorStoreES('', '.', environment=args.environment)
        
        # Query the tracking index
        query = {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"source_id": args.source_id}}
                    ]
                }
            },
            "sort": [{"processing_timestamp": {"order": "desc"}}],
            "size": args.limit
        }
        
        results = vector_store.es.search(
            index="pdf_processing_tracker",
            body=query
        )
        
        logger.info(f"📋 PDF Processing Status for Source: {args.source_id}")
        logger.info("=" * 50)
        
        if not results['hits']['hits']:
            logger.info("No processing records found.")
            return
        
        for hit in results['hits']['hits']:
            source = hit['_source']
            filename = source['pdf_filename']
            status = source['processing_status']
            timestamp = source['processing_timestamp'][:19]
            
            logger.info(f"{timestamp} | {filename} | {status}")
            
            if source.get('error_message'):
                logger.info(f"  Error: {source['error_message']}")
        
    except Exception as e:
        logger.error(f"Failed to get PDF status: {e}")
        logger.info("")


def test_cache_command(args):
    """Test cache functionality with sample data."""
    logger = get_logger("cache_cli")
    cache = SyncCache(cache_directory=args.cache_dir)
    
    logger.info("🧪 Testing Cache Functionality")
    logger.info("=" * 40)
    
    # Test content hashing
    test_content = "This is test content for caching"
    content_hash = cache.compute_content_hash(test_content)
    content_size = len(test_content.encode('utf-8'))
    
    logger.info(f"Test Content: {test_content}")
    logger.info(f"Content Hash: {content_hash[:16]}...")
    logger.info(f"Content Size: {content_size} bytes")
    
    # Test duplicate detection
    logger.info("\n🔍 Testing Duplicate Detection")
    duplicate_info = cache.is_duplicate("test-source-1", content_hash, content_size)
    logger.info(f"First check (should not be duplicate): {duplicate_info.is_duplicate}")
    
    duplicate_info2 = cache.is_duplicate("test-source-2", content_hash, content_size)
    logger.info(f"Second check (should be duplicate): {duplicate_info2.is_duplicate}")
    if duplicate_info2.is_duplicate:
        logger.info(f"Reason: {duplicate_info2.reason}")
    
    # Test caching
    logger.info("\n💾 Testing Content Caching")
    metadata = {"url": "http://example.com", "type": "test"}
    cache.cache_content("test-source-1", content_hash, content_size, metadata)
    
    cached = cache.get_cached_content("test-source-1")
    if cached:
        logger.info(f"Cached content retrieved: {cached.source_id}")
        logger.info(f"Metadata: {cached.metadata}")
    
    # Mark as processed
    cache.mark_processed("test-source-1", processed=True)
    cached_processed = cache.get_cached_content("test-source-1")
    logger.info(f"Marked as processed: {cached_processed.processed}")
    
    # Show final statistics
    logger.info("\n📊 Final Cache Statistics")
    stats = cache.get_cache_statistics()
    logger.info(f"Total Sources: {stats['total_sources']}")
    logger.info(f"Processed Sources: {stats['processed_sources']}")
    logger.info(f"Success Rate: {stats['success_rate']:.1f}%")


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(description="Sync Cache Management CLI")
    parser.add_argument("--cache-dir", default="./cache", help="Cache directory path")
    
    subparsers = parser.add_subparsers(dest="command", help="Available commands")
    
    # Stats command
    stats_parser = subparsers.add_parser("stats", help="Show cache statistics")
    
    # Duplicate summary command
    dup_parser = subparsers.add_parser("duplicates", help="Show duplicate detection summary")
    
    # Cleanup command
    cleanup_parser = subparsers.add_parser("cleanup", help="Clean up old cache entries")
    cleanup_parser.add_argument("--days", type=int, default=30, help="Days old threshold")
    
    # Logs command
    logs_parser = subparsers.add_parser("logs", help="Show sync operation logs")
    logs_parser.add_argument("--source-id", help="Filter by source ID")
    logs_parser.add_argument("--limit", type=int, default=20, help="Number of logs to show")
    
    # PDF discovery command
    pdf_discover_parser = subparsers.add_parser("pdf-discover", help="Discover and process PDFs from remote source")
    pdf_discover_parser.add_argument("--config-file", required=True, help="Sync configuration file")
    pdf_discover_parser.add_argument("--source-id", required=True, help="PDF source ID to process")
    pdf_discover_parser.add_argument("--environment", default="staging", choices=["staging", "production", "local"], help="Environment")
    pdf_discover_parser.add_argument("--temp-dir", help="Temporary directory for downloads")
    
    # PDF status command
    pdf_status_parser = subparsers.add_parser("pdf-status", help="Check PDF processing status")
    pdf_status_parser.add_argument("--source-id", required=True, help="Source ID to check")
    pdf_status_parser.add_argument("--environment", default="staging", choices=["staging", "production", "local"], help="Environment")
    pdf_status_parser.add_argument("--limit", type=int, default=20, help="Number of records to show")
    
    # Test command
    test_parser = subparsers.add_parser("test", help="Test cache functionality")
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    # Execute command
    if args.command == "stats":
        cache_stats_command(args)
    elif args.command == "duplicates":
        duplicate_summary_command(args)
    elif args.command == "cleanup":
        cache_cleanup_command(args)
    elif args.command == "logs":
        cache_logs_command(args)
    elif args.command == "pdf-discover":
        pdf_discover_command(args)
    elif args.command == "pdf-status":
        pdf_status_command(args)
    elif args.command == "test":
        test_cache_command(args)


if __name__ == "__main__":
    main() 