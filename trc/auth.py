"""Authentication logic for TRC CLI."""
from dataclasses import dataclass
from typing import Optional, Dict, Any
from trc.config import load_config, save_config, clear_config
import click


@dataclass
class AuthStatus:
    """
    Authentication status information.
    
    :param authenticated: Whether a user is logged in.
    :type authenticated: bool
    :param username: The logged-in username, if any.
    :type username: Optional[str]
    """
    authenticated: bool
    username: Optional[str] = None


def login(username: str) -> None:
    """
    Log in a user by storing their credentials in local config.
    
    :param username: The username to authenticate.
    :type username: str
    """
    config = {
        "authenticated": True,
        "username": username
    }
    save_config(config)


def logout() -> None:
    """Log out the current user by clearing the local config."""
    clear_config()


def get_auth_status() -> AuthStatus:
    """
    Get the current authentication status.
    
    :returns: Authentication status information.
    :rtype: AuthStatus
    """
    config = load_config()
    return AuthStatus(
        authenticated=config.get("authenticated", False),
        username=config.get("username")
    )


def is_authenticated() -> bool:
    """
    Check if a user is currently authenticated.
    
    :returns: True if authenticated, False otherwise.
    :rtype: bool
    """
    status = get_auth_status()
    return status.authenticated


def require_auth() -> AuthStatus:
    """
    Require authentication and return auth status if authenticated.
    
    :returns: Auth status information.
    :rtype: AuthStatus
    :raises SystemExit: If user is not authenticated.
    """
    status = get_auth_status()
    if not status.authenticated:
        click.echo("Error: Not authenticated. Please run 'trc auth login' first.")
        raise SystemExit(1)
    return status
