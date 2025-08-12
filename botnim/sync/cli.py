"""
CLI commands for managing the sync cache and duplicate detection.
"""

import argparse
import json

from ..config import get_logger
from .cache import SyncCache, DuplicateDetector
from .config import SyncConfig
from .pdf_discovery import process_pdf_source
from .embedding_processor import SyncEmbeddingProcessor
from .orchestrator import SyncOrchestratorCLI, SyncOrchestrator

from botnim.vector_store.vector_store_es import VectorStoreES
from botnim.cli import get_openai_client


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
        vector_store = VectorStoreES('', '.', environment=args.environment)
        
        # Initialize OpenAI client
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


def embedding_process_command(args):
    """Process documents for embedding generation."""
    logger = get_logger("embedding_cli")
    
    try:
        # Load sync configuration
        config = SyncConfig.from_yaml(args.config_file)
        
        logger.info(f"🔮 Processing embeddings for configuration: {config.name}")
        
        # Initialize components
        cache = SyncCache(cache_directory=args.cache_dir)
        
        # Initialize vector store
        vector_store = VectorStoreES('', '.', environment=args.environment)
        
        # Initialize OpenAI client
        openai_client = get_openai_client()
        
        # Initialize embedding processor
        embedding_processor = SyncEmbeddingProcessor(
            vector_store=vector_store,
            openai_client=openai_client,
            sync_cache=cache,
            embedding_cache_path=config.embedding_cache_path
        )
        
        # Load processed documents (this would come from sync workflow)
        # For now, we'll simulate with cached content
        processed_content = []
        
        # Get cached content for processing
        cache_entries = cache.get_all_cached_content()
        for entry in cache_entries:
            if entry.processed and not entry.error_message:
                processed_content.append({
                    'source_id': entry.source_id,
                    'content': entry.metadata.get('parsed_content', {}).get('text_content', ''),
                    'version_info': entry.metadata.get('version_info', {})
                })
        
        if not processed_content:
            logger.info("No processed content found for embedding")
            return
        
        # Process embeddings
        results = embedding_processor.process_sync_content(processed_content)
        
        # Display results
        logger.info("🔮 Embedding Processing Results")
        logger.info("=" * 40)
        logger.info(f"Total Documents: {results['total_documents']}")
        logger.info(f"Documents Needing Embedding: {results['documents_needing_embedding']}")
        logger.info(f"Successfully Processed: {results['processed_documents']}")
        
        embedding_results = results.get('embedding_results', {})
        if embedding_results.get('errors'):
            logger.info("\n❌ Errors:")
            for error in embedding_results['errors']:
                logger.info(f"  - {error}")
        
        logger.info("✅ Embedding processing completed")
        
    except Exception as e:
        logger.error(f"Embedding processing failed: {e}")


def embedding_stats_command(args):
    """Show embedding storage statistics."""
    logger = get_logger("embedding_cli")
    
    try:
        # Initialize vector store
        vector_store = VectorStoreES('', '.', environment=args.environment)
        
        # Initialize embedding processor
        cache = SyncCache(cache_directory=args.cache_dir)
        
        openai_client = get_openai_client()
        
        embedding_processor = SyncEmbeddingProcessor(
            vector_store=vector_store,
            openai_client=openai_client,
            sync_cache=cache,
            embedding_cache_path=args.cache_dir + "/embeddings.sqlite"
        )
        
        # Get statistics
        stats = embedding_processor.get_embedding_statistics()
        
        # Display statistics
        logger.info("🔮 Embedding Storage Statistics")
        logger.info("=" * 40)
        logger.info(f"Total Embeddings: {stats.get('total_embeddings', 0):,}")
        logger.info(f"Storage Size: {stats.get('storage_size_mb', 0):.2f} MB")
        logger.info(f"Index Name: {stats.get('index_name', 'N/A')}")
        
        model_dist = stats.get('model_distribution', {})
        if model_dist:
            logger.info("\nModel Distribution:")
            for model, count in model_dist.items():
                logger.info(f"  {model}: {count:,} embeddings")
        
        if 'error' in stats:
            logger.error(f"❌ Error: {stats['error']}")
        
    except Exception as e:
        logger.error(f"Failed to get embedding statistics: {e}")


def embedding_download_command(args):
    """Download embedding cache from cloud storage."""
    logger = get_logger("embedding_cli")
    
    try:
        # Initialize components
        vector_store = VectorStoreES('', '.', environment=args.environment)
        
        cache = SyncCache(cache_directory=args.cache_dir)
        
        openai_client = get_openai_client()
        
        embedding_processor = SyncEmbeddingProcessor(
            vector_store=vector_store,
            openai_client=openai_client,
            sync_cache=cache,
            embedding_cache_path=args.cache_file
        )
        
        # Download cache
        logger.info("⬇️ Downloading embedding cache from cloud storage...")
        results = embedding_processor.download_embedding_cache()
        
        # Display results
        if results['success']:
            logger.info(f"✅ Downloaded {results['embeddings_downloaded']:,} embeddings")
            logger.info(f"Cache file: {results['cache_file']}")
        else:
            logger.error(f"❌ Download failed: {results.get('error', 'Unknown error')}")
        
    except Exception as e:
        logger.error(f"Failed to download embedding cache: {e}")


def embedding_upload_command(args):
    """Upload embedding cache to cloud storage."""
    logger = get_logger("embedding_cli")
    
    try:
        # Initialize components
        vector_store = VectorStoreES('', '.', environment=args.environment)
        
        cache = SyncCache(cache_directory=args.cache_dir)
        
        openai_client = get_openai_client()
        
        embedding_processor = SyncEmbeddingProcessor(
            vector_store=vector_store,
            openai_client=openai_client,
            sync_cache=cache,
            embedding_cache_path=args.cache_file
        )
        
        # Upload cache
        logger.info("⬆️ Uploading embedding cache to cloud storage...")
        results = embedding_processor.upload_embedding_cache()
        
        # Display results
        if results['success']:
            logger.info(f"✅ Uploaded {results['embeddings_uploaded']:,} embeddings")
        else:
            logger.error(f"❌ Upload failed: {results.get('error', 'Unknown error')}")
        
    except Exception as e:
        logger.error(f"Failed to upload embedding cache: {e}")


def sync_orchestrate_command(args):
    """Run comprehensive sync orchestration."""
    logger = get_logger("orchestrator_cli")
    
    try:
        cli = SyncOrchestratorCLI()
        
        # Run sync orchestration
        logger.info(f"🚀 Starting sync orchestration for config: {args.config_file}")
        logger.info(f"Environment: {args.environment}")
        
        summary = cli.run_sync_from_config(args.config_file, args.environment)
        
        # Print comprehensive summary
        cli.print_summary(summary)
        
        # Exit with appropriate code
        if summary.failed_sources > 0:
            logger.warning(f"⚠️ Sync completed with {summary.failed_sources} failed sources")
            exit(1)
        else:
            logger.info("✅ Sync orchestration completed successfully")
            
    except Exception as e:
        logger.error(f"❌ Sync orchestration failed: {e}")
        exit(1)


def sync_stats_command(args):
    """Show comprehensive sync statistics."""
    logger = get_logger("orchestrator_cli")
    
    try:
        config = SyncConfig.from_yaml(args.config_file)
        logger.info(f"📊 Sync Statistics for: {config.name}")
        
        orchestrator = SyncOrchestrator(config, args.environment)
        
        try:
            # Get comprehensive statistics
            stats = orchestrator.get_sync_statistics()
            
            # Display statistics
            logger.info("=" * 50)
            logger.info("📈 COMPREHENSIVE SYNC STATISTICS")
            logger.info("=" * 50)
            
            # Configuration info
            logger.info(f"Configuration: {stats.get('config_name', 'N/A')}")
            logger.info(f"Environment: {stats.get('environment', 'N/A')}")
            logger.info(f"Total Sources: {stats.get('total_sources', 0)}")
            logger.info(f"Enabled Sources: {stats.get('enabled_sources', 0)}")
            
            # Cache statistics
            cache_stats = stats.get('cache_statistics', {})
            if cache_stats:
                logger.info(f"\n💾 Cache Statistics:")
                logger.info(f"  Total Sources: {cache_stats.get('total_sources', 0)}")
                logger.info(f"  Processed Sources: {cache_stats.get('processed_sources', 0)}")
                logger.info(f"  Success Rate: {cache_stats.get('success_rate', 0):.1f}%")
                logger.info(f"  Cache Size: {cache_stats.get('cache_size_mb', 0):.2f} MB")
            
            # Embedding statistics
            embedding_stats = stats.get('embedding_statistics', {})
            if embedding_stats:
                logger.info(f"\n🔮 Embedding Statistics:")
                logger.info(f"  Total Embeddings: {embedding_stats.get('total_embeddings', 0):,}")
                logger.info(f"  Storage Size: {embedding_stats.get('storage_size_mb', 0):.2f} MB")
                
                model_dist = embedding_stats.get('model_distribution', {})
                if model_dist:
                    logger.info(f"  Model Distribution:")
                    for model, count in model_dist.items():
                        logger.info(f"    {model}: {count:,} embeddings")
            
            # Version statistics
            logger.info(f"\n📝 Version Statistics:")
            logger.info(f"  Tracked Versions: {stats.get('version_count', 0)}")
            
        finally:
            orchestrator.cleanup()
            
    except Exception as e:
        logger.error(f"Failed to get sync statistics: {e}")


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
    
    # Embedding commands
    embed_process_parser = subparsers.add_parser("embedding-process", help="Process documents for embedding generation")
    embed_process_parser.add_argument("--config-file", required=True, help="Sync configuration file")
    embed_process_parser.add_argument("--environment", default="staging", choices=["staging", "production", "local"], help="Environment")
    
    embed_stats_parser = subparsers.add_parser("embedding-stats", help="Show embedding storage statistics")
    embed_stats_parser.add_argument("--environment", default="staging", choices=["staging", "production", "local"], help="Environment")
    
    embed_download_parser = subparsers.add_parser("embedding-download", help="Download embedding cache from cloud")
    embed_download_parser.add_argument("--cache-file", required=True, help="Local cache file path")
    embed_download_parser.add_argument("--environment", default="staging", choices=["staging", "production", "local"], help="Environment")
    
    embed_upload_parser = subparsers.add_parser("embedding-upload", help="Upload embedding cache to cloud")
    embed_upload_parser.add_argument("--cache-file", required=True, help="Local cache file path")
    embed_upload_parser.add_argument("--environment", default="staging", choices=["staging", "production", "local"], help="Environment")
    
    # Orchestrator commands
    sync_orchestrate_parser = subparsers.add_parser("orchestrate", help="Run comprehensive sync orchestration")
    sync_orchestrate_parser.add_argument("--config-file", required=True, help="Sync configuration file")
    sync_orchestrate_parser.add_argument("--environment", default="staging", choices=["staging", "production", "local"], help="Environment")
    
    sync_stats_parser = subparsers.add_parser("sync-stats", help="Show comprehensive sync statistics")
    sync_stats_parser.add_argument("--config-file", required=True, help="Sync configuration file")
    sync_stats_parser.add_argument("--environment", default="staging", choices=["staging", "production", "local"], help="Environment")
    
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
    elif args.command == "embedding-process":
        embedding_process_command(args)
    elif args.command == "embedding-stats":
        embedding_stats_command(args)
    elif args.command == "embedding-download":
        embedding_download_command(args)
    elif args.command == "embedding-upload":
        embedding_upload_command(args)
    elif args.command == "orchestrate":
        sync_orchestrate_command(args)
    elif args.command == "sync-stats":
        sync_stats_command(args)


if __name__ == "__main__":
    main() 