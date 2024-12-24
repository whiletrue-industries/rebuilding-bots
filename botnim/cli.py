import click
from .sync import sync_agents
from .benchmark.runner import run_benchmarks

@click.group()
def cli():
    """A simple CLI tool."""
    pass

# Sync command, receives two arguments: production/staging and a list of bots to sync ('budgetkey'/'takanon' or 'all')
@cli.command()
@click.argument('environment', type=click.Choice(['production', 'staging']))
@click.argument('bots', type=click.Choice(['budgetkey', 'takanon', 'all']))
@click.option('--replace-context', is_flag=True, default=False, help="Replace all contexts")
@click.option('--replace-common-knowledge', is_flag=True, default=False, help="Replace only common knowledge contexts")
def sync(environment, bots, replace_context, replace_common_knowledge):
    """Sync bots to Airtable."""
    if replace_context and replace_common_knowledge:
        click.echo("Warning: --replace-common-knowledge will be ignored since --replace-context is True")
    click.echo(f"Syncing {bots} to {environment}")
    sync_agents(environment, bots, replace_context=replace_context, replace_common_knowledge=replace_common_knowledge)

# Run benchmarks command, receives three arguments: production/staging, a list of bots to run benchmarks on ('budgetkey'/'takanon' or 'all') and whether to run benchmarks on the production environment to work locally (true/false)
@cli.command()
@click.argument('environment', type=click.Choice(['production', 'staging']))
@click.argument('bots', type=click.Choice(['budgetkey', 'takanon', 'all']))
@click.argument('local', type=click.BOOL)
@click.option('--reuse-answers', type=click.BOOL, default=False)
@click.option('--select', type=click.STRING, default='failed', help='failed/all/AirTable record ID')
@click.option('--concurrency', type=click.INT, default=None)
def benchmarks(environment, bots, local, reuse_answers, select, concurrency):
    """Run benchmarks on bots."""
    click.echo(f"Running benchmarks on {bots} in {environment} (save results locally: {local}, reuse answers: {reuse_answers}, select: {select})")
    run_benchmarks(environment, bots, local, reuse_answers, select, concurrency)

def main():
    cli()

if __name__ == '__main__':
    main()
