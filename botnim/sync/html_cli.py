#!/usr/bin/env python3
"""
CLI tool for HTML fetching and parsing operations.

This tool provides command-line interface for testing HTML content fetching,
parsing, and processing functionality.
"""

import argparse
import json
import sys
from pathlib import Path
from typing import Optional

from ..config import get_logger
from .config import SyncConfig, ContentSource, HTMLSourceConfig, SourceType, VersioningStrategy
from .cache import SyncCache
from .html_fetcher import HTMLFetcher, HTMLProcessor, fetch_and_parse_html

logger = get_logger("html_cli")


def fetch_single_url(url: str, selector: Optional[str] = None, 
                    output_file: Optional[str] = None, 
                    cache_dir: str = "./cache") -> None:
    """Fetch and parse a single HTML URL."""
    logger.info(f"Fetching HTML from: {url}")
    
    try:
        # Create cache
        cache = SyncCache(cache_dir)
        
        # Fetch and parse
        result = fetch_and_parse_html(url, selector)
        
        if "error" in result:
            logger.error(f"Failed to fetch HTML: {result['error']}")
            sys.exit(1)
        
        # Output results
        if output_file:
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(result, f, indent=2, ensure_ascii=False)
            logger.info(f"Results saved to: {output_file}")
        else:
            # Pretty print to console
            print("\n" + "="*50)
            print("HTML FETCHING RESULTS")
            print("="*50)
            
            print(f"\n📄 Metadata:")
            metadata = result.get('metadata', {})
            print(f"  Title: {metadata.get('title', 'N/A')}")
            print(f"  Description: {metadata.get('description', 'N/A')}")
            print(f"  Language: {metadata.get('language', 'N/A')}")
            
            print(f"\n🔗 Links ({len(result.get('links', []))}):")
            for link in result.get('links', [])[:5]:  # Show first 5
                print(f"  - {link['text']}: {link['url']}")
            if len(result.get('links', [])) > 5:
                print(f"  ... and {len(result.get('links', [])) - 5} more")
            
            print(f"\n📊 Structure:")
            structure = result.get('structure', {})
            print(f"  Headings: {len(structure.get('headings', []))}")
            print(f"  Lists: {len(structure.get('lists', []))}")
            print(f"  Tables: {len(structure.get('tables', []))}")
            print(f"  Images: {len(structure.get('images', []))}")
            
            print(f"\n📝 Content Preview:")
            text_content = result.get('text_content', '')
            preview = text_content[:500] + "..." if len(text_content) > 500 else text_content
            print(f"  {preview}")
            
            print(f"\n📄 Source Metadata:")
            source_meta = result.get('source_metadata', {})
            print(f"  Source ID: {source_meta.get('source_id', 'N/A')}")
            print(f"  Content Size: {source_meta.get('content_size', 0)} bytes")
            print(f"  Version Hash: {source_meta.get('version_hash', 'N/A')[:16]}...")
        
    except Exception as e:
        logger.error(f"Error fetching HTML: {e}")
        sys.exit(1)


def process_config_sources(config_file: str, cache_dir: str = "./cache", 
                          source_ids: Optional[list] = None) -> None:
    """Process HTML sources from a configuration file."""
    logger.info(f"Processing HTML sources from config: {config_file}")
    
    try:
        # Load configuration
        config = SyncConfig.from_yaml(config_file)
        
        # Filter HTML sources
        html_sources = [s for s in config.sources if s.type == SourceType.HTML]
        
        if source_ids:
            html_sources = [s for s in html_sources if s.id in source_ids]
        
        if not html_sources:
            logger.warning("No HTML sources found in configuration")
            return
        
        logger.info(f"Found {len(html_sources)} HTML sources to process")
        
        # Create cache and processor
        cache = SyncCache(cache_dir)
        processor = HTMLProcessor(cache)
        
        try:
            # Process sources
            results = processor.process_sources(html_sources)
            
            # Display results
            print("\n" + "="*50)
            print("HTML PROCESSING RESULTS")
            print("="*50)
            
            summary = results['summary']
            print(f"\n📊 Summary:")
            print(f"  Total Sources: {summary['total_sources']}")
            print(f"  Processed: {summary['processed_count']}")
            print(f"  Skipped: {summary['skipped_count']}")
            print(f"  Errors: {summary['error_count']}")
            
            if results['processed']:
                print(f"\n✅ Processed Sources:")
                for item in results['processed']:
                    print(f"  - {item['source_id']} ({item['content_size']} bytes)")
            
            if results['skipped']:
                print(f"\n⏭️  Skipped Sources:")
                for item in results['skipped']:
                    print(f"  - {item['source_id']}: {item['reason']}")
            
            if results['errors']:
                print(f"\n❌ Errors:")
                for item in results['errors']:
                    print(f"  - {item['source_id']}: {item['error']}")
        
        finally:
            processor.close()
    
    except Exception as e:
        logger.error(f"Error processing HTML sources: {e}")
        sys.exit(1)


def test_html_parsing(html_file: str, selector: Optional[str] = None) -> None:
    """Test HTML parsing with a local file."""
    logger.info(f"Testing HTML parsing with file: {html_file}")
    
    try:
        # Read HTML file
        with open(html_file, 'r', encoding='utf-8') as f:
            html_content = f.read()
        
        # Create temporary source
        source = ContentSource(
            id="test-file",
            name="Test HTML File",
            type=SourceType.HTML,
            html_config=HTMLSourceConfig(
                url=f"file://{html_file}",
                selector=selector
            )
        )
        
        # Create temporary cache
        cache = SyncCache("./temp_cache")
        fetcher = HTMLFetcher(cache)
        
        try:
            # Parse content
            parsed_content = fetcher.parse_html_content(source, html_content)
            
            # Display results
            print("\n" + "="*50)
            print("HTML PARSING TEST RESULTS")
            print("="*50)
            
            print(f"\n📄 Metadata:")
            metadata = parsed_content.get('metadata', {})
            print(f"  Title: {metadata.get('title', 'N/A')}")
            print(f"  Description: {metadata.get('description', 'N/A')}")
            
            print(f"\n🔗 Links ({len(parsed_content.get('links', []))}):")
            for link in parsed_content.get('links', [])[:3]:
                print(f"  - {link['text']}: {link['url']}")
            
            print(f"\n📊 Structure:")
            structure = parsed_content.get('structure', {})
            print(f"  Headings: {len(structure.get('headings', []))}")
            print(f"  Lists: {len(structure.get('lists', []))}")
            print(f"  Tables: {len(structure.get('tables', []))}")
            
            print(f"\n📝 Content Preview:")
            text_content = parsed_content.get('text_content', '')
            preview = text_content[:300] + "..." if len(text_content) > 300 else text_content
            print(f"  {preview}")
        
        finally:
            fetcher.close()
            # Clean up temp cache
            import shutil
            if Path("./temp_cache").exists():
                shutil.rmtree("./temp_cache")
    
    except Exception as e:
        logger.error(f"Error testing HTML parsing: {e}")
        sys.exit(1)


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="HTML fetching and parsing CLI tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Fetch a single URL
  python -m botnim.sync.html_cli fetch https://example.com --selector "#content"
  
  # Process HTML sources from config
  python -m botnim.sync.html_cli process config.yaml --source-ids source1 source2
  
  # Test parsing with local file
  python -m botnim.sync.html_cli test test.html --selector "#main"
        """
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Fetch command
    fetch_parser = subparsers.add_parser('fetch', help='Fetch and parse a single HTML URL')
    fetch_parser.add_argument('url', help='URL to fetch')
    fetch_parser.add_argument('--selector', help='CSS selector for content extraction')
    fetch_parser.add_argument('--output', help='Output file for results (JSON)')
    fetch_parser.add_argument('--cache-dir', default='./cache', help='Cache directory')
    
    # Process command
    process_parser = subparsers.add_parser('process', help='Process HTML sources from configuration')
    process_parser.add_argument('config_file', help='Configuration file path')
    process_parser.add_argument('--cache-dir', default='./cache', help='Cache directory')
    process_parser.add_argument('--source-ids', nargs='+', help='Specific source IDs to process')
    
    # Test command
    test_parser = subparsers.add_parser('test', help='Test HTML parsing with local file')
    test_parser.add_argument('html_file', help='HTML file to test')
    test_parser.add_argument('--selector', help='CSS selector for content extraction')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        sys.exit(1)
    
    try:
        if args.command == 'fetch':
            fetch_single_url(args.url, args.selector, args.output, args.cache_dir)
        elif args.command == 'process':
            process_config_sources(args.config_file, args.cache_dir, args.source_ids)
        elif args.command == 'test':
            test_html_parsing(args.html_file, args.selector)
        else:
            logger.error(f"Unknown command: {args.command}")
            sys.exit(1)
    
    except KeyboardInterrupt:
        logger.info("Operation cancelled by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main() 