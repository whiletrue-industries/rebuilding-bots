import click
import sys
from pathlib import Path
import json
import logging

from .vector_store.vector_store_es import VectorStoreES
from .vector_store.search_modes import SEARCH_MODES, DEFAULT_SEARCH_MODE
from . import sync
from .benchmark.runner import run_benchmarks
from .benchmark.evaluate_metrics_cli import evaluate
from .config import AVAILABLE_BOTS, VALID_ENVIRONMENTS, DEFAULT_ENVIRONMENT, is_production
from .query import run_query, get_available_indexes, get_index_fields, format_mapping
from .cli_assistant import assistant_main
from .config import SPECS, get_logger
from .document_parser.html_processor.process_document import PipelineRunner, PipelineConfig
from .document_parser.html_processor.extract_structure import extract_structure_from_html, build_nested_structure, get_openai_client
from .document_parser.html_processor.extract_content import extract_content_from_html
from .document_parser.html_processor.generate_markdown_files import generate_markdown_from_json
from .document_parser.html_processor.pipeline_config import Environment
from .document_parser.pdf_processor.pdf_pipeline import PDFExtractionPipeline
from .document_parser.pdf_processor.google_sheets_service import GoogleSheetsService
from .sync import HTMLFetcher, HTMLProcessor, fetch_and_parse_html
from .sync.cache import SyncCache
from .sync.config import SyncConfig, ContentSource

logger = get_logger(__name__)

@click.group()
def cli():
    """A simple CLI tool."""
    pass

# Sync command, receives two arguments: production/staging and a list of bots to sync ('budgetkey'/'takanon' or 'all')
@cli.command(name='sync')
@click.argument('environment', type=click.Choice(VALID_ENVIRONMENTS))
@click.argument('bots', type=click.Choice(['budgetkey', 'takanon', 'all']))
@click.option('--replace-context', type=str, help='Replace existing context with a specific context name or use "all" to replace all contexts')
@click.option('--backend', type=click.Choice(['es', 'openai']), default='openai', help='Vector store backend')
@click.option('--reindex', is_flag=True, default=False, help='Force reindexing to update mapping changes')
def sync(environment, bots, replace_context, backend, reindex):
    """Sync bots to Airtable."""
    click.echo(f"Syncing {bots} to {environment}")
    sync.sync_agents(environment, bots, backend=backend, replace_context=replace_context, reindex=reindex)

# Run benchmarks command, receives three arguments: production/staging, a list of bots to run benchmarks on ('budgetkey'/'takanon' or 'all') and whether to run benchmarks on the production environment to work locally (true/false)
@cli.command(name='benchmarks')
@click.argument('environment', type=click.Choice(VALID_ENVIRONMENTS))
@click.argument('bots', type=click.Choice(['budgetkey', 'takanon', 'all']))
@click.option('--local', is_flag=True, default=False, help='Run benchmarks locally')
@click.option('--reuse-answers', is_flag=True, default=False)
@click.option('--select', type=click.STRING, default='failed', help='failed/all/AirTable record ID')
@click.option('--concurrency', type=click.INT, default=None)
def benchmarks(environment, bots, local, reuse_answers, select, concurrency):
    """Run benchmarks on bots."""
    click.echo(f"Running benchmarks on {bots} in {environment} (save results locally: {local}, reuse answers: {reuse_answers}, select: {select})")
    run_benchmarks(environment, bots, local, reuse_answers, select, concurrency)

@cli.group(name='query')
def query_group():
    """Query the vector store."""
    pass

def mirror_brackets(text: str) -> str:
    """Replace bracket characters with their mirrored counterparts."""
    bracket_map = str.maketrans("()[]{}", ")(][}{")
    return text.translate(bracket_map)

def reverse_lines(text: str) -> str:
    """Reverse the order of lines in the text."""
    lines = text.splitlines()
    reversed_lines = [line[::-1] for line in lines]
    return "\n".join(reversed_lines)

@query_group.command(name='search')
@click.argument('environment', type=click.Choice(VALID_ENVIRONMENTS))
@click.argument('bot', type=click.Choice(AVAILABLE_BOTS))
@click.argument('context', type=click.STRING)
@click.argument('query_text', type=click.STRING)
@click.option('--num-results', '-n', type=int, help='Number of results to return. If not provided, uses the default for the selected search mode.')
@click.option('--full', '-f', is_flag=True, help='Show full content of results')
@click.option('--rtl', is_flag=True, help='Display results in right-to-left order')
@click.option('--explain', is_flag=True, help='Show detailed scoring explanation for results')
@click.option('--search-mode', type=click.Choice(list(SEARCH_MODES.keys())), help='Use a specific search mode (see "list-modes" for details)')
def search(environment: str, bot: str, context: str, query_text: str, num_results: int, full: bool, rtl: bool, explain: bool, search_mode: str):
    """Search the vector store with the given query.
    If --num-results/-n is not provided, the default for the selected search mode is used.
    Use --search-mode to select a specific search mode (see 'list-modes' for details).
    """
    logger.info(f"Searching {bot}/{context} in {environment} with query: '{query_text}', num_results: {num_results}, search_mode: {search_mode}")
    try:
        vector_store_id = VectorStoreES.encode_index_name(bot, context, environment)
        # Use the registry and canonical default for mode selection
        mode = SEARCH_MODES.get(search_mode, DEFAULT_SEARCH_MODE) if search_mode else DEFAULT_SEARCH_MODE
        search_results = run_query(
            store_id=vector_store_id, 
            query_text=query_text, 
            num_results=num_results, 
            format="text",
            explain=explain,
            search_mode=mode
        )
        if rtl:
            search_results = reverse_lines(mirror_brackets(search_results))
        click.echo(search_results)
    except Exception as e:
        click.echo(f"Error: {str(e)}", err=True)

@query_group.command(name='list-indexes')
@click.argument('environment', type=click.Choice(VALID_ENVIRONMENTS))
@click.option('--bot', type=click.Choice(AVAILABLE_BOTS), help='Filter indexes by bot name')
@click.option('--rtl', is_flag=True, help='Display results in right-to-left order')
def list_indexes(environment: str, bot: str, rtl: bool):
    """List all available indexes in the vector store."""
    try:
        indexes = get_available_indexes(environment, bot)
        click.echo("Available indexes:")
        for index in indexes:
            if rtl:
                index = index[::-1]
            click.echo(index)
    except Exception as e:
        click.echo(f"Error: {str(e)}", err=True)
        raise

@query_group.command(name='show-fields')
@click.argument('environment', type=click.Choice(VALID_ENVIRONMENTS))
@click.argument('bot', type=click.Choice(AVAILABLE_BOTS))
@click.argument('context', type=click.STRING)
@click.option('--rtl', is_flag=True, help='Display results in right-to-left order')
def show_fields(environment: str, bot: str, context: str, rtl: bool):
    """Show the fields/structure of an index."""
    try:
        mapping = get_index_fields(environment, bot, context)
        formatted = format_mapping(mapping)
        if rtl:
            formatted = reverse_lines(formatted)
        click.echo(formatted)
    except Exception as e:
        click.echo(f"Error: {str(e)}", err=True)

@query_group.command(name='list-modes')
def list_modes():
    """List all available search modes and their default settings."""
    click.echo("Available search modes:")
    for name, config in SEARCH_MODES.items():
        click.echo(f"- {name}: {getattr(config, 'description', '')}")
        click.echo(f"    Default num_results: {getattr(config, 'num_results', 7)}")
    click.echo("\nUse --search-mode <MODE> with 'search' to select a mode.")

@cli.command(name='assistant')
@click.option('--assistant-id', type=click.STRING, help='ID of the assistant to chat with')
@click.option('--openapi-spec', type=click.STRING, default=None, help='either "budgetkey" or "takanon"')
@click.option('--rtl', is_flag=True, help='Display results in right-to-left order')
@click.option('--environment', type=click.Choice(VALID_ENVIRONMENTS), default=DEFAULT_ENVIRONMENT,
              help='Environment to use for vector search')
def assistant(assistant_id, openapi_spec, rtl, environment):
    """Start an interactive chat with an OpenAI assistant."""
    logger.info(f"Starting assistant chat with assistant_id={assistant_id}, openapi_spec={openapi_spec}, environment={environment}")
    try:
        assistant_main(assistant_id, openapi_spec, rtl, environment)
    except Exception as e:
        logger.error(f"Error in assistant chat: {e}", exc_info=True)
        raise

# Add evaluate command to main CLI
cli.add_command(evaluate)

@cli.group(name='sync')
def sync_group():
    """Automated sync infrastructure commands."""
    pass

@sync_group.group(name='html')
def html_group():
    """HTML content fetching and processing commands."""
    pass

@html_group.command(name='fetch')
@click.argument('url')
@click.option('--selector', help='CSS selector to extract specific content')
@click.option('--encoding', default='utf-8', help='Content encoding')
@click.option('--timeout', type=int, default=30, help='Request timeout in seconds')
@click.option('--retry-attempts', type=int, default=3, help='Number of retry attempts')
def html_fetch(url, selector, encoding, timeout, retry_attempts):
    """Fetch and parse HTML content from a URL."""
    try:
        # Create a temporary source configuration
        from .sync.config import HTMLSourceConfig
        html_config = HTMLSourceConfig(
            url=url,
            selector=selector,
            encoding=encoding,
            timeout=timeout,
            retry_attempts=retry_attempts
        )
        
        source = ContentSource(
            id="temp-source",
            name="Temporary Source",
            description="Temporary source for CLI testing",
            type="html",
            html_config=html_config,
            versioning_strategy="hash",
            fetch_strategy="direct",
            enabled=True,
            priority=1,
            tags=[]
        )
        
        # Initialize cache and fetcher
        cache = SyncCache()
        fetcher = HTMLFetcher(cache)
        
        # Fetch and process
        success, parsed_content, version_info = fetcher.process_html_source(source)
        
        if success:
            click.echo("✅ Successfully fetched and parsed HTML content")
            click.echo(f"📄 Content size: {version_info.content_size} bytes")
            click.echo(f"🔗 Version hash: {version_info.version_hash}")
            click.echo(f"📝 Text content preview: {parsed_content['text_content'][:200]}...")
        else:
            click.echo("❌ Failed to fetch HTML content", err=True)
            
        fetcher.close()
        
    except Exception as e:
        click.echo(f"Error: {str(e)}", err=True)

@html_group.command(name='process')
@click.argument('config_file')
@click.option('--source-ids', multiple=True, help='Process specific source IDs (default: all enabled sources)')
@click.option('--verbose', is_flag=True, help='Enable verbose logging')
def html_process(config_file, source_ids, verbose):
    """Process HTML sources from a configuration file."""
    try:
        # Load configuration
        config = SyncConfig.from_yaml(config_file)
        
        # Filter sources
        html_sources = [s for s in config.sources if s.type == "html" and s.enabled]
        
        if source_ids:
            html_sources = [s for s in html_sources if s.id in source_ids]
        
        if not html_sources:
            click.echo("No enabled HTML sources found in configuration")
            return
        
        # Initialize processor
        cache = SyncCache()
        processor = HTMLProcessor(cache)
        
        # Process sources
        results = processor.process_sources(html_sources)
        
        # Display results
        click.echo(f"📊 Processing Summary:")
        click.echo(f"   Total sources: {results['summary']['total_sources']}")
        click.echo(f"   Processed: {results['summary']['processed_count']}")
        click.echo(f"   Skipped: {results['summary']['skipped_count']}")
        click.echo(f"   Errors: {results['summary']['error_count']}")
        
        if results['processed']:
            click.echo("\n✅ Successfully processed:")
            for result in results['processed']:
                click.echo(f"   • {result['source_id']}: {result['content_size']} bytes")
        
        if results['skipped']:
            click.echo("\n⏭️ Skipped:")
            for result in results['skipped']:
                click.echo(f"   • {result['source_id']}: {result['reason']}")
        
        if results['errors']:
            click.echo("\n❌ Errors:")
            for result in results['errors']:
                click.echo(f"   • {result['source_id']}: {result['error']}")
        
        processor.close()
        
    except Exception as e:
        click.echo(f"Error: {str(e)}", err=True)

@sync_group.group(name='cache')
def cache_group():
    """Sync cache management commands."""
    pass

@cache_group.command(name='stats')
def cache_stats():
    """Show sync cache statistics."""
    try:
        cache = SyncCache()
        stats = cache.get_cache_statistics()
        
        click.echo("📊 Sync Cache Statistics:")
        click.echo(f"   Total sources: {stats['total_sources']}")
        click.echo(f"   Processed sources: {stats['processed_sources']}")
        click.echo(f"   Error sources: {stats['error_sources']}")
        click.echo(f"   Success rate: {stats['success_rate']:.1f}%")
        click.echo(f"   Total duplicates: {stats['total_duplicates']}")
        click.echo(f"   High duplicate count: {stats['high_duplicate_count']}")
        click.echo(f"   Cache size: {stats['cache_size_mb']:.2f} MB")
        
    except Exception as e:
        click.echo(f"Error: {str(e)}", err=True)

@cache_group.command(name='cleanup')
@click.option('--older-than', type=int, help='Remove entries older than N days')
@click.option('--dry-run', is_flag=True, help='Show what would be removed without actually removing')
def cache_cleanup(older_than, dry_run):
    """Clean up old cache entries."""
    try:
        cache = SyncCache()
        
        if older_than:
            if dry_run:
                click.echo(f"🔍 Dry run: Would remove entries older than {older_than} days")
                click.echo("Note: Dry run mode not implemented in cache cleanup")
            else:
                removed_count = cache.cleanup_old_entries(older_than)
                click.echo(f"🗑️ Removed {removed_count} entries older than {older_than} days")
        else:
            click.echo("Please specify --older-than to clean up old entries")
        
    except Exception as e:
        click.echo(f"Error: {str(e)}", err=True)

@cli.command(name='process-document')
@click.argument('input_html_file')
@click.argument('output_base_dir')
@click.option('--content-type', default='סעיף')
@click.option('--environment', default='staging')
@click.option('--model', default='gpt-4.1')
@click.option('--max-tokens', type=int, default=None)
@click.option('--dry-run', is_flag=True)
@click.option('--overwrite', is_flag=True)
@click.option('--generate-markdown', is_flag=True)
@click.option('--mediawiki-mode', is_flag=True)
def process_document_cmd(input_html_file, output_base_dir, content_type, environment, model, max_tokens, dry_run, overwrite, generate_markdown, mediawiki_mode):
    """Run the full document processing pipeline."""
    config = PipelineConfig(
        input_html_file=input_html_file,
        output_base_dir=output_base_dir,
        content_type=content_type,
        environment=Environment(environment),  # Convert string to enum
        model=model,
        max_tokens=max_tokens,
        dry_run=dry_run,
        overwrite_existing=overwrite,
        mediawiki_mode=mediawiki_mode,
    )
    runner = PipelineRunner(config)
    runner.run(generate_markdown=generate_markdown)

@cli.command(name='extract-structure')
@click.argument('input_file')
@click.argument('output_file')
@click.option('--environment', default='staging')
@click.option('--model', default='gpt-4.1')
@click.option('--max-tokens', type=int, default=None)
@click.option('--pretty', is_flag=True)
@click.option('--mark-type', default=None)
def extract_structure_cmd(input_file, output_file, environment, model, max_tokens, pretty, mark_type):
    """Extract hierarchical structure from HTML using OpenAI API."""
    # Read input HTML
    input_path = Path(input_file)
    with open(input_path, 'r', encoding='utf-8') as f:
        html_text = f.read()
    client = get_openai_client(environment)
    structure_items = extract_structure_from_html(html_text, client, model, max_tokens, mark_type)
    nested_structure = build_nested_structure(structure_items)
    output_data = {
        "metadata": {
            "input_file": str(input_path),
            "document_name": input_path.stem,
            "environment": environment,
            "model": model,
            "max_tokens": max_tokens,
            "total_items": len(structure_items),
            "structure_type": "nested_hierarchy",
            "mark_type": mark_type
        },
        "structure": nested_structure
    }
    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, 'w', encoding='utf-8') as f:
        if pretty:
            json.dump(output_data, f, ensure_ascii=False, indent=2)
        else:
            json.dump(output_data, f, ensure_ascii=False)

@cli.command(name='extract-content')
@click.argument('html_file')
@click.argument('structure_file')
@click.argument('content_type')
@click.option('--output', '-o', default=None)
@click.option('--mediawiki-mode', is_flag=True)
def extract_content_cmd(html_file, structure_file, content_type, output, mediawiki_mode):
    """Extract content for specific section types from HTML files."""
    output_path = output or Path(structure_file).with_name(Path(structure_file).stem + '_content.json')
    extract_content_from_html(
        html_path=html_file,
        structure_path=structure_file,
        content_type=content_type,
        output_path=output_path,
        mediawiki_mode=mediawiki_mode
    )

@cli.command(name='generate-markdown-files')
@click.argument('json_file')
@click.option('--output-dir', '-o', default=None)
@click.option('--write-files', is_flag=True)
@click.option('--dry-run', is_flag=True)
def generate_markdown_files_cmd(json_file, output_dir, write_files, dry_run):
    """Generate markdown files from a JSON structure with content."""
    generate_markdown_from_json(
        json_path=json_file,
        output_dir=output_dir,
        write_files=write_files,
        dry_run=dry_run
    )

@cli.command(name='pdf-extract')
@click.argument('config_file')
@click.argument('input_dir')
@click.option('--source', help='Process specific source (default: process all)')
@click.option('--environment', default='staging', help='API environment (default: staging)')
@click.option('--verbose', is_flag=True, help='Enable verbose logging')
@click.option('--no-metrics', is_flag=True, help='Disable performance metrics collection')
@click.option('--upload-to-sheets', is_flag=True, help='Upload results to Google Sheets after processing')
@click.option('--spreadsheet-id', help='Google Sheets spreadsheet ID for upload')
@click.option('--replace-sheet', is_flag=True, help='Replace existing sheet instead of appending')
@click.option('--use-adc', is_flag=True, help='Use Application Default Credentials instead of service account key')
@click.option('--credentials-path', help='Path to service account credentials file (if not using ADC)')
@click.option('--pdfs-only', is_flag=True, help='Process PDFs only, no Google Sheets upload')
@click.option('--upload-only', is_flag=True, help='Upload CSV files only, no PDF processing')
def pdf_extract_cmd(config_file, input_dir, source, environment, verbose, no_metrics, 
                   upload_to_sheets, spreadsheet_id, replace_sheet, use_adc, credentials_path,
                   pdfs_only, upload_only):
    """Extract structured data from PDFs using LLM with CSV-based contract.
    
    Input directory should contain:
    - input.csv (optional, existing data)
    - *.pdf files
    - *.pdf.metadata.json files (optional)
    
    Output will be written to output.csv in the same directory.
    
    MODES:
    - Default: Process PDFs and optionally upload to Google Sheets
    - --pdfs-only: Process PDFs only, no cloud storage
    - --upload-only: Upload existing CSV files to Google Sheets only
    """
    # Setup logging
    log_level = logging.INFO if verbose else logging.WARNING
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    try:
        # Validate arguments
        if not Path(config_file).exists():
            click.echo(f"Error: Configuration file not found: {config_file}", err=True)
            sys.exit(1)
        
        if not Path(input_dir).exists():
            click.echo(f"Error: Input directory not found: {input_dir}", err=True)
            sys.exit(1)
        
        # Determine workflow mode
        if pdfs_only:
            # PDF processing only - CSV contract pattern
            click.echo("🚀 PDF processing only (CSV contract pattern)")
            success = _process_pdfs_only(config_file, input_dir, environment, verbose, no_metrics, source)
        elif upload_only:
            # Google Sheets upload only
            if not spreadsheet_id:
                click.echo("Error: --spreadsheet-id is required for upload-only mode", err=True)
                sys.exit(1)
            click.echo("📊 Google Sheets upload only")
            success = _upload_to_sheets_only(input_dir, spreadsheet_id, use_adc, credentials_path, replace_sheet, verbose)
        else:
            # Complete workflow with separation of concerns
            # Auto-enable upload if spreadsheet_id is provided
            if spreadsheet_id and not upload_to_sheets:
                upload_to_sheets = True
                click.echo("📊 Auto-enabling Google Sheets upload (spreadsheet-id provided)")
            
            if upload_to_sheets and not spreadsheet_id:
                click.echo("Error: --spreadsheet-id is required when --upload-to-sheets is specified", err=True)
                sys.exit(1)
            
            click.echo("🔄 Complete workflow (PDF processing + optional Google Sheets upload)")
            success = _process_and_upload(
                config_file, input_dir, source, environment, verbose, no_metrics,
                upload_to_sheets, spreadsheet_id, replace_sheet, use_adc, credentials_path
            )
        
        if success:
            click.echo("✅ Operation completed successfully")
        else:
            click.echo("❌ Operation completed with errors")
            sys.exit(1)
            
    except Exception as e:
        click.echo(f"Error: {str(e)}", err=True)
        sys.exit(1)

def _process_pdfs_only(config_file: str, input_dir: str, environment: str, verbose: bool, no_metrics: bool, source: str = None) -> bool:
    """
    Process PDFs only - CSV contract pattern.
    
    This demonstrates the separation of concerns:
    - Input: input.csv (optional) + PDF files + metadata
    - Output: output.csv
    - No cloud storage coupling
    """
    try:
        # Initialize pipeline (no Google Sheets coupling)
        openai_client = get_openai_client(environment)
        pipeline = PDFExtractionPipeline(config_file, openai_client, enable_metrics=not no_metrics)
        
        # Process directory following CSV contract
        success = pipeline.process_directory(input_dir, source_filter=source)
        
        if success:
            click.echo("✅ PDF processing completed successfully")
            click.echo("📁 Output: output.csv")
            click.echo("📊 Metrics: pipeline_metrics.json")
            
            # Show brief summary even in non-verbose mode
            if not verbose:
                # Try to get basic stats from output file
                output_csv_path = Path(input_dir) / "output.csv"
                if output_csv_path.exists():
                    try:
                        import csv
                        with open(output_csv_path, 'r', encoding='utf-8') as f:
                            reader = csv.DictReader(f)
                            records = list(reader)
                            click.echo(f"📈 Extracted {len(records)} records")
                            
                            # Show source breakdown if available
                            if records and 'source_name' in records[0]:
                                source_counts = {}
                                for record in records:
                                    source_name = record.get('source_name', 'Unknown')
                                    source_counts[source_name] = source_counts.get(source_name, 0) + 1
                                
                                if len(source_counts) > 1:
                                    click.echo("📋 Sources processed:")
                                    for source_name, count in source_counts.items():
                                        click.echo(f"   • {source_name}: {count} records")
                    
                    except Exception:
                        # If we can't read the CSV, just show the basic success message
                        pass
                
                # Check for failure indicators in logs or metrics
                metrics_path = Path(input_dir) / "pipeline_metrics.json"
                if metrics_path.exists():
                    try:
                        import json
                        with open(metrics_path, 'r', encoding='utf-8') as f:
                            metrics_data = json.load(f)
                        
                        # Check for failures in metrics
                        failed_extractions = metrics_data.get('pipeline_summary', {}).get('failed_extractions', 0)
                        if failed_extractions > 0:
                            click.echo(f"⚠️ {failed_extractions} files failed to process")
                            click.echo("   Use --verbose for detailed failure information")
                    
                    except Exception:
                        # If we can't read metrics, continue without failure info
                        pass
            else:
                click.echo("🔍 Detailed summary available in logs above")
        else:
            click.echo("❌ PDF processing failed")
        
        return success
        
    except Exception as e:
        click.echo(f"❌ PDF processing error: {e}", err=True)
        return False

def _upload_to_sheets_only(input_dir: str, spreadsheet_id: str, use_adc: bool, 
                          credentials_path: str, replace_sheet: bool, verbose: bool) -> bool:
    """
    Upload CSV files to Google Sheets only - no PDF processing.
    
    This demonstrates the separation of concerns:
    - Input: CSV files in directory
    - Output: Google Sheets upload
    """
    try:
        # Initialize Google Sheets service (no PDF coupling)
        sheets_service = GoogleSheetsService(
            credentials_path=credentials_path,
            use_adc=use_adc
        )
        
        # Upload CSV files (prioritizing output.csv from CSV contract pattern)
        results = sheets_service.upload_directory_csvs(
            input_dir, spreadsheet_id, replace_existing=replace_sheet, prefer_output_csv=True
        )
        
        # Report results
        successful = sum(1 for success in results.values() if success)
        total = len(results)
        
        click.echo(f"📈 Upload results: {successful}/{total} files uploaded successfully")
        
        for sheet_name, success in results.items():
            status = "✅" if success else "❌"
            click.echo(f"{status} {sheet_name}")
        
        return successful == total
        
    except Exception as e:
        click.echo(f"❌ Google Sheets upload error: {e}", err=True)
        return False

def _process_and_upload(config_file: str, input_dir: str, source: str, environment: str, 
                       verbose: bool, no_metrics: bool, upload_to_sheets: bool, 
                       spreadsheet_id: str, replace_sheet: bool, use_adc: bool, 
                       credentials_path: str) -> bool:
    """
    Process PDFs and optionally upload to Google Sheets as separate steps.
    
    This demonstrates the complete workflow with separation of concerns:
    1. Process PDFs -> output.csv
    2. Upload output.csv -> Google Sheets (if requested)
    """
    try:
        # Step 1: Process PDFs
        click.echo("📄 Step 1: Processing PDFs...")
        pdf_success = _process_pdfs_only(config_file, input_dir, environment, verbose, no_metrics, source)
        
        if not pdf_success:
            click.echo("❌ PDF processing failed, skipping Google Sheets upload", err=True)
            return False
        
        # Step 2: Upload to Google Sheets (if requested)
        if upload_to_sheets:
            click.echo("📊 Step 2: Uploading to Google Sheets...")
            sheets_success = _upload_to_sheets_only(
                input_dir, spreadsheet_id, use_adc, credentials_path, replace_sheet, verbose
            )
            
            if not sheets_success:
                click.echo("❌ Google Sheets upload failed", err=True)
                return False
        
        return True
        
    except Exception as e:
        click.echo(f"❌ Complete workflow error: {e}", err=True)
        return False

def main():
    cli()

if __name__ == '__main__':
    main()
