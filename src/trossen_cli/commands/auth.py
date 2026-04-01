"""Authentication commands."""

from typing import Annotated

import typer

from .. import auth as auth_module

app = typer.Typer(help="Authentication commands")


@app.command("login")
def login(
    token: Annotated[
        str | None,
        typer.Option("--token", "-t", help="API token"),
    ] = None,
) -> None:
    """
    Log in to Trossen Cloud.

    Token can also be provided via the TROSSEN_TOKEN environment variable.

    """
    auth_module.login_command(token)


@app.command("logout")
def logout() -> None:
    """
    Log out and clear stored credentials.
    """
    auth_module.logout_command()


@app.command("status")
def status() -> None:
    """
    Show authentication status.
    """
    auth_module.status_command()
