"""Authentication commands and token management."""

import os
import stat

import keyring
import typer
from rich.prompt import Prompt

from .config import get_token_file
from .output import console
from .types import StoredToken

KEYRING_SERVICE = "trossen-cli"
KEYRING_USERNAME = "tokens"


def _store_token_keyring(token: str) -> bool:
    """
    Store token in system keyring.
    """
    try:
        data = StoredToken(token=token).model_dump_json()
        keyring.set_password(KEYRING_SERVICE, KEYRING_USERNAME, data)
        return True
    except Exception:
        return False


def _store_token_file(token: str) -> None:
    """
    Store token in file with restricted permissions.
    """
    token_file = get_token_file()
    data = StoredToken(token=token).model_dump_json()

    with open(token_file, "w") as f:
        f.write(data)

    # Set file permissions to owner-only read/write
    os.chmod(token_file, stat.S_IRUSR | stat.S_IWUSR)


def _load_token_keyring() -> str | None:
    """
    Load token from system keyring.
    """
    try:
        data = keyring.get_password(KEYRING_SERVICE, KEYRING_USERNAME)
        if data:
            return StoredToken.model_validate_json(data).token
    except Exception:
        pass
    return None


def _load_token_file() -> str | None:
    """
    Load token from file.
    """
    token_file = get_token_file()
    if token_file.exists():
        try:
            with open(token_file) as f:
                data = f.read()
            return StoredToken.model_validate_json(data).token
        except Exception:
            pass
    return None


def store_token(token: str) -> None:
    """
    Store token securely (keyring with file fallback).
    """
    if not _store_token_keyring(token):
        _store_token_file(token)


def load_token() -> str | None:
    """
    Load token from storage. Env var takes priority.
    """
    if token := os.environ.get("TROSSEN_TOKEN"):
        return token

    # Try keyring first, then file
    token = _load_token_keyring()
    if token is None:
        token = _load_token_file()

    return token


def get_token() -> str | None:
    """
    Get the stored API token.
    """
    return load_token()


def clear_token() -> None:
    """
    Clear stored token.
    """
    # Clear from keyring
    try:
        keyring.delete_password(KEYRING_SERVICE, KEYRING_USERNAME)
    except Exception:
        pass

    # Clear from file
    token_file = get_token_file()
    if token_file.exists():
        token_file.unlink()


def require_auth() -> str:
    """
    Require authentication and return API token.
    """
    token = get_token()
    if not token:
        console.print("[error]Not authenticated. Please run 'trc auth login' first.[/error]")
        raise typer.Exit(1)
    return token


# CLI Commands


def login_command(token: str | None = None) -> None:
    """
    Log in to Trossen Cloud by storing an API token.
    """
    token = token or os.environ.get("TROSSEN_TOKEN")

    if not token:
        token = Prompt.ask("API token", password=True)

    store_token(token)
    prefix = token[:10] if len(token) >= 10 else token
    console.print(f"[success]Token stored ({prefix}...)[/success]")


def logout_command() -> None:
    """
    Log out and clear stored credentials.
    """
    clear_token()
    console.print("[success]Logged out[/success]")


def status_command() -> None:
    """
    Show authentication status.
    """
    token = get_token()
    if not token:
        console.print("[warning]Not authenticated.[/warning]")
        raise typer.Exit(1)

    from .api_client import ApiError, SyncApiClient

    try:
        with SyncApiClient() as client:
            data = client.get("/users/me")
        username = data.get("username", "unknown")
        console.print(f"[success]Authenticated[/success] as [bold]{username}[/bold]")
    except ApiError as e:
        console.print(f"[error]Authentication failed: {e.message}[/error]")
        raise typer.Exit(1)
