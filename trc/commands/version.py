"""Version command for TRC CLI."""
import click


@click.command()
def version():
    """Display the TRC CLI version."""
    click.echo("trc version 0.0.1")
