"""Authentication commands for TRC CLI."""
import click
from trc.auth import login as auth_login, logout as auth_logout, get_auth_status


@click.group()
def auth():
    """Authentication commands."""
    pass


@auth.command()
def login():
    """Log in to Trossen Robotics Cloud."""
    username = click.prompt("Username")
    
    auth_login(username)
    click.echo(f"Successfully logged in as {username}")


@auth.command()
def logout():
    """Log out from Trossen Robotics Cloud."""
    auth_logout()
    click.echo("Successfully logged out")


@auth.command()
def status():
    """Check authentication status."""
    auth_status = get_auth_status()
    
    if auth_status.authenticated:
        click.echo(f"Logged in as: {auth_status.username}")
    else:
        click.echo("Not authenticated")
