"""Configuration commands."""

from typing import Annotated

import typer

from ..config import Config, get_config, load_config, reset_config, save_config
from ..output import console, print_error, print_success

app = typer.Typer(help="View and manage CLI configuration.")

# Flat map of settable keys to (section, field, type)
CONFIG_KEYS: dict[str, tuple[str, str, type]] = {
    "upload.chunk_size_mb": ("upload", "chunk_size_mb", int),
    "upload.parallel_parts": ("upload", "parallel_parts", int),
    "upload.parallel_files": ("upload", "parallel_files", int),
    "download.parallel_files": ("download", "parallel_files", int),
    "download.stream_chunk_size": ("download", "stream_chunk_size", int),
}


@app.command("show")
def show_command() -> None:
    """
    Show current configuration.
    """
    config = get_config()

    console.print("\n[heading]Upload[/heading]")
    console.print(f"  chunk_size_mb      {config.upload.chunk_size_mb}")
    console.print(f"  parallel_parts     {config.upload.parallel_parts}")
    console.print(f"  parallel_files     {config.upload.parallel_files}")

    console.print("\n[heading]Download[/heading]")
    console.print(f"  parallel_files     {config.download.parallel_files}")
    console.print(f"  stream_chunk_size  {config.download.stream_chunk_size}")
    console.print()


@app.command("set")
def set_command(
    key: Annotated[str, typer.Argument(help="Config key (e.g. upload.chunk_size_mb)")],
    value: Annotated[str, typer.Argument(help="Value to set")],
) -> None:
    """
    Set a configuration value.
    """
    if key not in CONFIG_KEYS:
        print_error(f"Unknown key: {key}")
        console.print(f"[muted]Valid keys: {', '.join(sorted(CONFIG_KEYS))}[/muted]")
        raise typer.Exit(1)

    section, field, field_type = CONFIG_KEYS[key]

    try:
        parsed = field_type(value)
    except ValueError:
        print_error(f"Invalid value for {key}: expected {field_type.__name__}")
        raise typer.Exit(1)

    if parsed <= 0:
        print_error("Value must be positive")
        raise typer.Exit(1)

    # Load fresh from disk, apply change, save
    config = load_config()
    setattr(getattr(config, section), field, parsed)
    save_config(config)
    reset_config()

    print_success(f"{key} = {parsed}")


@app.command("reset")
def reset_command(
    force: Annotated[bool, typer.Option("--force", "-f", help="Skip confirmation")] = False,
) -> None:
    """
    Reset configuration to defaults.
    """
    if not force:
        typer.confirm("Reset all settings to defaults?", abort=True)

    config = Config()
    save_config(config)
    reset_config()

    print_success("Configuration reset to defaults")
