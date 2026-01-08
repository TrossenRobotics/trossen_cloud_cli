"""User command for TRC CLI."""
import click
from trc.auth import get_auth_status


@click.command()
def user():
    """Display the currently authenticated user."""
    auth_status = get_auth_status()
    
    if auth_status.authenticated:
        click.echo(f"User: {auth_status.username}")
    else:
        click.echo("Not authenticated. Please run 'trc auth login' first.")
