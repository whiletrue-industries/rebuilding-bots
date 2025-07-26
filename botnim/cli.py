import click
import sys
from pathlib import Path
import json
import logging

from botnim.vector_store.vector_store_es import VectorStoreES
from botnim.vector_store.search_modes import SEARCH_MODES, DEFAULT_SEARCH_MODE
from .sync import sync_agents
from .benchmark.runner import run_benchmarks
from .benchmark.evaluate_metrics_cli import evaluate
from .config import AVAILABLE_BOTS, VALID_ENVIRONMENTS, DEFAULT_ENVIRONMENT, is_production
from .query import run_query, get_available_indexes, get_index_fields, format_mapping
from .cli_assistant import assistant_main
from .config import SPECS, get_logger
from .document_parser.dynamic_extractions.process_document import PipelineRunner, PipelineConfig
from .document_parser.dynamic_extractions.extract_structure import extract_structure_from_html, build_nested_structure, get_openai_client
from .document_parser.dynamic_extractions.extract_content import extract_content_from_html
from .document_parser.dynamic_extractions.generate_markdown_files import generate_markdown_from_json
from .document_parser.dynamic_extractions.pipeline_config import Environment
from .document_parser.dynamic_extractions.pdf_extraction.pdf_pipeline import PDFExtractionPipeline

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
    sync_agents(environment, bots, backend=backend, replace_context=replace_context, reindex=reindex)

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
@click.option('--source', help='Process specific source (default: process all)')
@click.option('--output-dir', default='.', help='Output directory for CSV files')
@click.option('--upload-sheets', is_flag=True, help='Upload results to Google Sheets')
@click.option('--sheets-credentials', help='Path to Google Sheets credentials JSON')
@click.option('--spreadsheet-id', help='Google Sheets spreadsheet ID')
@click.option('--replace-sheet', is_flag=True, help='Replace existing sheet content')
@click.option('--environment', default='staging', help='API environment (default: staging)')
@click.option('--verbose', is_flag=True, help='Enable verbose logging')
@click.option('--no-metrics', is_flag=True, help='Disable performance metrics collection')
def pdf_extract_cmd(config_file, source, output_dir, upload_sheets, sheets_credentials, spreadsheet_id, replace_sheet, environment, verbose, no_metrics):
    """Extract structured data from PDFs using LLM and sync to Google Sheets."""
    # Setup logging
    log_level = logging.INFO if verbose else logging.WARNING
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    try:
        # Initialize OpenAI client
        openai_client = get_openai_client(environment)
        
        # Initialize pipeline
        pipeline = PDFExtractionPipeline(
            config_file, 
            openai_client, 
            output_dir, 
            enable_metrics=not no_metrics
        )
        
        # Process sources
        if source:
            success = pipeline.process_source(
                source, upload_sheets, sheets_credentials,
                spreadsheet_id, replace_sheet
            )
        else:
            success = pipeline.process_all_sources(
                upload_sheets, sheets_credentials,
                spreadsheet_id, replace_sheet
            )
        
        if success:
            click.echo("PDF extraction completed successfully")
            # Save and display metrics
            pipeline.save_metrics()
            pipeline.print_performance_summary()
        else:
            click.echo("PDF extraction completed with errors")
            # Still save metrics even if there were errors
            pipeline.save_metrics()
            pipeline.print_performance_summary()
            sys.exit(1)
            
    except Exception as e:
        click.echo(f"Error: {str(e)}", err=True)
        sys.exit(1)

def main():
    cli()

if __name__ == '__main__':
    main()
