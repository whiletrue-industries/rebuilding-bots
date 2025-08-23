import click

from botnim.fetch_and_process import fetch_and_process
from botnim.vector_store.vector_store_es import VectorStoreES
from botnim.vector_store.search_modes import SEARCH_MODES, DEFAULT_SEARCH_MODE
from .sync import sync_agents
from .benchmark.runner import run_benchmarks
from .benchmark.evaluate_metrics_cli import evaluate
from .config import AVAILABLE_BOTS, VALID_ENVIRONMENTS, DEFAULT_ENVIRONMENT, is_production
from .query import run_query, get_available_indexes, get_index_fields, format_mapping
from .cli_assistant import assistant_main
from .config import SPECS, get_logger
from .document_parser.wikitext.process_document import WikitextProcessor, WikitextProcessorConfig
from .document_parser.wikitext.pipeline_config import Environment

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

@cli.command(name='fetch-and-process')
@click.argument('bot', type=click.Choice(AVAILABLE_BOTS + ['all']))
@click.argument('context', type=click.STRING)
@click.argument('kind', type=click.Choice(['all', 'wikitext']))
@click.option('--environment', default='staging')
def fetch_and_process_(environment, bot, context):
    """Fetch and process documents from various sources."""
    logger.info(f"Fetching and processing documents in environment: {environment}")
    try:
        fetch_and_process(environment, bot, context)
    except Exception as e:
        logger.error(f"Error in fetch-and-process: {e}", exc_info=True)
        raise

@cli.command(name='process-wikitext')
@click.argument('input_url')
@click.argument('output_base_dir')
@click.option('--content-type', default='סעיף')
@click.option('--environment', default='staging')
@click.option('--model', default='gpt-4.1')
@click.option('--max-tokens', type=int, default=None)
@click.option('--generate-markdown', is_flag=True)
def process_wikitext(input_url, output_base_dir, content_type, environment, model, max_tokens, generate_markdown):
    """Run the full document processing pipeline."""
    config = WikitextProcessorConfig(
        input_url=input_url,
        output_base_dir=output_base_dir,
        content_type=content_type,
        environment=Environment(environment),  # Convert string to enum
        model=model,
        max_tokens=max_tokens,
    )
    runner = WikitextProcessor(config)
    runner.run(generate_markdown=generate_markdown)

def main():
    cli()

if __name__ == '__main__':
    main()
