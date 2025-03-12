import click
from .sync import sync_agents
from .benchmark.runner import run_benchmarks
from .config import AVAILABLE_BOTS
from .query import run_query, get_available_indexes, format_result, get_index_fields, format_mapping
from .cli_assistant import assistant_main
from .config import SPECS


@click.group()
def cli():
    """A simple CLI tool."""
    pass

# Sync command, receives two arguments: production/staging and a list of bots to sync ('budgetkey'/'takanon' or 'all')
@cli.command(name='sync')
@click.argument('environment', type=click.Choice(['production', 'staging']))
@click.argument('bots', type=click.Choice(['budgetkey', 'takanon', 'all']))
@click.option('--replace-context', is_flag=True, help='Replace existing context')
@click.option('--backend', type=click.Choice(['es', 'openai']), default='openai', help='Vector store backend')
@click.option('--extract-metadata', is_flag=True, help='Extract metadata from content (may be slow)')
def sync(environment, bots, replace_context, backend, extract_metadata):
    """Sync bots to Airtable."""
    click.echo(f"Syncing {bots} to {environment}")
    sync_agents(environment, bots, backend=backend, replace_context=replace_context, extract_metadata=extract_metadata)

# Run benchmarks command, receives three arguments: production/staging, a list of bots to run benchmarks on ('budgetkey'/'takanon' or 'all') and whether to run benchmarks on the production environment to work locally (true/false)
@cli.command(name='benchmarks')
@click.argument('environment', type=click.Choice(['production', 'staging']))
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
@click.argument('environment', type=click.Choice(['production', 'staging']))
@click.argument('bot', type=click.Choice(AVAILABLE_BOTS), default='takanon')
@click.argument('context', type=click.STRING)
@click.argument('query_text', type=str)
@click.option('--num-results', type=int, default=7, help='Number of results to return')
@click.option('--full', '-f', is_flag=True, help='Show full content of results')
@click.option('--rtl', is_flag=True, help='Display results in right-to-left order')
def search(environment: str, bot: str, context: str, query_text: str, num_results: int, full: bool, rtl: bool):
    """Search the vector store with the given query."""
    try:
        search_results = run_query(query_text, environment, bot, context, num_results)
        for result in search_results:
            formatted_result = format_result(result, show_full=full)
            if rtl:
                formatted_result = reverse_lines(mirror_brackets(formatted_result))
            click.echo(formatted_result)
    except Exception as e:
        click.echo(f"Error: {str(e)}", err=True)
        raise click.Abort()

@query_group.command(name='list-indexes')
@click.argument('environment', type=click.Choice(['production', 'staging']))
@click.option('--bot', type=click.Choice(AVAILABLE_BOTS), default='takanon', 
              help='Bot to list indexes for')
@click.option('--rtl', is_flag=True, help='Display results in right-to-left order')
def list_indexes(environment: str, bot: str, rtl: bool):
    """List all available indexes in the vector store."""
    try:
        indexes = get_available_indexes(environment, bot)
        click.echo("Available indexes:")
        for index in indexes:
            index_display = index[::-1] if rtl else index
            click.echo(f"  - {mirror_brackets(index_display)}")
    except Exception as e:
        click.echo(f"Error: {str(e)}", err=True)
        raise click.Abort()

@query_group.command(name='show-fields')
@click.argument('environment', type=click.Choice(['production', 'staging']))
@click.argument('bot', type=click.Choice(AVAILABLE_BOTS), default='takanon')
@click.argument('context', type=click.STRING)
@click.option('--rtl', is_flag=True, help='Display results in right-to-left order')
def show_fields(environment: str, bot: str, context: str, rtl: bool):
    """Show all available fields in the index."""
    try:
        mapping = get_index_fields(environment, bot, context)
        formatted_mapping = format_mapping(mapping)
        if rtl:
            formatted_mapping = reverse_lines(mirror_brackets(formatted_mapping))
        click.echo(f"\nFields in index for bot '{bot}', context '{context}':")
        click.echo(formatted_mapping)
    except Exception as e:
        click.echo(f"Error: {str(e)}", err=True)
        raise click.Abort()

@cli.command(name='assistant')
@click.option('--assistant-id', type=str, help='ID of the assistant to chat with')
@click.option('--openapi-spec', type=str, default='budgetkey', help='either "budgetkey" or "takanon"')
@click.option('--rtl', is_flag=True, help='Enable RTL support for Hebrew/Arabic')
def assistant(assistant_id, openapi_spec, rtl):
    """Start an interactive chat with an OpenAI assistant."""
    assistant_main(assistant_id, openapi_spec, rtl)


def main():
    cli()

if __name__ == '__main__':
    main()
