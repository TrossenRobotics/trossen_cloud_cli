"""List command for TRC CLI."""
import click
from ..auth import require_auth


@click.command()
def ls():
    """List cloud resources (requires authentication)."""
    # Require authentication before proceeding
    auth_status = require_auth()
    
    # Display mock cloud resources
    click.echo("Cloud resources:")
    click.echo("  datasets/")
    click.echo("  models/")
    click.echo("  logs/")
