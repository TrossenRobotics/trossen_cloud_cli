"""Authentication logic for TRC CLI."""
from typing import Optional, Dict, Any
from .config import load_config, save_config, clear_config


def login(username: str, org: str) -> None:
    """
    Log in a user by storing their credentials in local config.
    
    Args:
        username: The username to authenticate.
        org: The organization name.
    """
    config = {
        "authenticated": True,
        "username": username,
        "org": org
    }
    save_config(config)


def logout() -> None:
    """Log out the current user by clearing the local config."""
    clear_config()


def get_auth_status() -> Dict[str, Any]:
    """
    Get the current authentication status.
    
    Returns:
        Dictionary with authentication status, including:
        - authenticated (bool): Whether a user is logged in
        - username (str, optional): The logged-in username
        - org (str, optional): The user's organization
    """
    config = load_config()
    return {
        "authenticated": config.get("authenticated", False),
        "username": config.get("username"),
        "org": config.get("org")
    }


def is_authenticated() -> bool:
    """
    Check if a user is currently authenticated.
    
    Returns:
        True if authenticated, False otherwise.
    """
    status = get_auth_status()
    return status.get("authenticated", False)


def require_auth() -> Dict[str, Any]:
    """
    Require authentication and return auth status if authenticated.
    
    Returns:
        Auth status dictionary.
        
    Raises:
        SystemExit: If user is not authenticated.
    """
    status = get_auth_status()
    if not status["authenticated"]:
        import click
        click.echo("Error: Not authenticated. Please run 'trc auth login' first.")
        raise SystemExit(1)
    return status
