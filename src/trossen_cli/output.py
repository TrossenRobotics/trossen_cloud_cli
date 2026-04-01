"""Output utilities for CLI messages."""

from rich.console import Console
from rich.theme import Theme

theme = Theme(
    {
        "success": "green",
        "error": "bold red",
        "warning": "yellow",
        "info": "cyan",
        "heading": "bold",
        "label": "bold",
        "muted": "dim",
        "table.id": "cyan",
    }
)

console = Console(theme=theme)


def format_size(size_bytes: int | float) -> str:
    """
    Format a size in bytes to human-readable string.
    """
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if abs(size_bytes) < 1024.0:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.1f} PB"


def print_success(message: str) -> None:
    """
    Print a success message.
    """
    console.print(f"[success]{message}[/success]")


def print_error(message: str) -> None:
    """
    Print an error message.
    """
    console.print(f"[error]error[/error]: {message}")


def print_warning(message: str) -> None:
    """
    Print a warning message.
    """
    console.print(f"[warning]warning[/warning]: {message}")


def print_info(message: str) -> None:
    """
    Print an info message.
    """
    console.print(f"[muted]{message}[/muted]")
