import click
from .sync import sync_agents
from .benchmark.runner import run_benchmarks
from .config import SPECS
from .query import run_query, get_available_indexes, format_result, get_available_bots


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
def sync(environment, bots, replace_context, backend):
    """Sync bots to Airtable."""
    click.echo(f"Syncing {bots} to {environment}")
    sync_agents(environment, bots, backend=backend,replace_context=replace_context)

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

@query_group.command(name='search')
@click.argument('query_text', type=str)
@click.option('--bot', type=click.Choice(get_available_bots()), default='takanon', 
              help='Bot to query')
@click.option('--results', type=int, default=7, help='Number of results to return')
def search(query_text: str, bot: str, results: int):
    """Search the vector store with the given query."""
    try:
        search_results = run_query(query_text, bot, results)
        for result in search_results:
            click.echo(format_result(result))
    except Exception as e:
        click.echo(f"Error: {str(e)}", err=True)
        raise click.Abort()

@query_group.command(name='list-indexes')
@click.option('--bot', type=click.Choice(get_available_bots()), default='takanon', 
              help='Bot to list indexes for')
def list_indexes(bot: str):
    """List all available indexes in the vector store."""
    try:
        indexes = get_available_indexes(bot)
        click.echo("Available indexes:")
        for index in indexes:
            click.echo(f"  - {index}")
    except Exception as e:
        click.echo(f"Error: {str(e)}", err=True)
        raise click.Abort()

def main():
    cli()

if __name__ == '__main__':
    main()
