import click
from .sync import sync_agents
from .benchmark.runner import run_benchmarks
from .kb.download_sources import download_sources
from .config import SPECS
import shutil

@click.group()
def cli():
    """A simple CLI tool."""
    pass

# Sync command, receives two arguments: production/staging and a list of bots to sync ('budgetkey'/'takanon' or 'all')
@cli.command()
@click.argument('environment', type=click.Choice(['production', 'staging']))
@click.argument('bots', type=click.Choice(['budgetkey', 'takanon', 'all']))
@click.option('--replace-context', is_flag=True, default=False, help='Replace existing context')
def sync(environment, bots, replace_context=False):
    """Sync bots to Airtable."""
    click.echo(f"Syncing {bots} to {environment}")
    sync_agents(environment, bots, replace_context=replace_context)

# Run benchmarks command, receives three arguments: production/staging, a list of bots to run benchmarks on ('budgetkey'/'takanon' or 'all') and whether to run benchmarks on the production environment to work locally (true/false)
@cli.command()
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

@cli.command()
@click.argument('bots', type=click.Choice(['budgetkey', 'takanon', 'all']), default='all')
def download(bots):
    """Download external sources for specific or all bots"""
    click.echo(f"Downloading external sources for {bots}...")
    
    # First remove existing .md files or directories
    if bots == 'all':
        patterns = ['*/common-knowledge.md']
    else:
        patterns = [f'{bots}/common-knowledge.md']
        
    for pattern in patterns:
        for path in SPECS.glob(pattern):
            if path.is_file():
                path.unlink()
            elif path.is_dir():
                shutil.rmtree(path)
            
    download_sources(SPECS, bots)

def main():
    cli()

if __name__ == '__main__':
    main()
