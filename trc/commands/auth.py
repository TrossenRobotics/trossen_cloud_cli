"""Authentication commands for TRC CLI."""
import click
from ..auth import login as auth_login, logout as auth_logout, get_auth_status


@click.group()
def auth():
    """Authentication commands."""
    pass


@auth.command()
def login():
    """Log in to Trossen Robotics Cloud."""
    username = click.prompt("Username")
    org = click.prompt("Organization", default="trossen-robotics")
    
    auth_login(username, org)
    click.echo(f"Successfully logged in as {username} (org: {org})")


@auth.command()
def logout():
    """Log out from Trossen Robotics Cloud."""
    auth_logout()
    click.echo("Successfully logged out")


@auth.command()
def status():
    """Check authentication status."""
    auth_status = get_auth_status()
    
    if auth_status["authenticated"]:
        click.echo(f"Logged in as: {auth_status['username']}")
        click.echo(f"Organization: {auth_status['org']}")
    else:
        click.echo("Not authenticated")
