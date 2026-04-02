"""Model commands."""

import asyncio
import json
from pathlib import Path
from typing import Annotated

import typer
from rich.table import Table

from ..api_client import ApiClient, ApiError
from ..auth import require_auth
from ..download import download_model
from ..output import console, print_error, print_info, print_success
from ..types import PrivacyLevel
from ..upload import UploadError, create_and_upload_model

app = typer.Typer(help="Manage models")


def is_user_name_format(identifier: str) -> bool:
    """
    Check if identifier is in <user>/<name> format.
    """
    return "/" in identifier and not identifier.startswith("/")


async def resolve_model_identifier(client: ApiClient, identifier: str) -> dict:
    """
    Resolve a model identifier (UUID or user/name) to model info.
    """
    if is_user_name_format(identifier):
        # user/name format
        return await client.get(f"/models/{identifier}")
    else:
        # UUID format
        return await client.get(f"/models/{identifier}")


@app.command("upload")
def upload(
    path: Annotated[
        Path,
        typer.Argument(
            help="Path to the model directory or file to upload",
            exists=True,
            resolve_path=True,
        ),
    ],
    name: Annotated[
        str,
        typer.Option("--name", "-n", help="Model name"),
    ],
    privacy: Annotated[
        PrivacyLevel,
        typer.Option("--privacy", "-p", help="Privacy level"),
    ] = PrivacyLevel.PRIVATE,
    base_model_id: Annotated[
        str | None,
        typer.Option("--base-model-id", "-b", help="Base model ID (for fine-tuned models)"),
    ] = None,
    metadata: Annotated[
        str | None,
        typer.Option("--metadata", "-m", help="JSON metadata string"),
    ] = None,
) -> None:
    """
    Upload a model to Trossen Cloud.
    """
    require_auth()

    # Parse metadata if provided
    metadata_dict = None
    if metadata:
        try:
            metadata_dict = json.loads(metadata)
        except json.JSONDecodeError:
            print_error("Invalid JSON metadata")
            raise typer.Exit(1)

    try:
        model = asyncio.run(
            create_and_upload_model(
                name=name,
                local_path=path,
                privacy=privacy.value,
                base_model_id=base_model_id,
                metadata=metadata_dict,
            )
        )
        console.print(f"[bold]ID:[/bold]   {model['id']}")
        console.print(f"[bold]Name:[/bold] {name}")

    except KeyboardInterrupt:
        raise typer.Exit(1)
    except UploadError as e:
        print_error(str(e))
        raise typer.Exit(1)


@app.command("download")
def download(
    model_id: Annotated[
        str,
        typer.Argument(help="Model ID (UUID or <user>/<name>) to download"),
    ],
    output_dir: Annotated[
        Path,
        typer.Argument(help="Output directory"),
    ],
) -> None:
    """
    Download a model from Trossen Cloud.
    """
    require_auth()

    async def do_download():
        async with ApiClient() as client:
            # Resolve identifier to get the actual ID
            model = await resolve_model_identifier(client, model_id)
            actual_id = model["id"]
        # Download using the resolved ID
        await download_model(actual_id, output_dir)

    try:
        asyncio.run(do_download())
        console.print(f"[bold]Path:[/bold] {output_dir}")

    except KeyboardInterrupt:
        print_error("Download interrupted")
        raise typer.Exit(1)
    except ApiError as e:
        if e.status_code == 404:
            print_error(f"Model '{model_id}' not found")
        else:
            print_error(f"Download failed: {e.message}")
        raise typer.Exit(1)
    except Exception as e:
        print_error(f"Download failed: {e}")
        raise typer.Exit(1)


@app.command("view")
def view(
    path: Annotated[
        str,
        typer.Argument(help="Model path in <user>/<name> format (e.g., trossen/act-base)"),
    ],
) -> None:
    """
    View a model by user/name path.
    """
    require_auth()

    if not is_user_name_format(path):
        print_error("Invalid format. Use <user>/<name> (e.g., trossen/act-base)")
        raise typer.Exit(1)

    async def fetch():
        async with ApiClient() as client:
            return await resolve_model_identifier(client, path)

    try:
        model = asyncio.run(fetch())
        _display_model_info(model)

    except ApiError as e:
        if e.status_code == 404:
            print_error(f"Model '{path}' not found")
        else:
            print_error(f"Failed to get model: {e.message}")
        raise typer.Exit(1)


@app.command("list")
def list_models(
    mine: Annotated[
        bool,
        typer.Option("--mine", help="Show only your models"),
    ] = False,
    derived_from: Annotated[
        str | None,
        typer.Option("--derived-from", help="Show models derived from a parent model (UUID)"),
    ] = None,
    limit: Annotated[
        int,
        typer.Option("--limit", "-l", help="Maximum number of models to show"),
    ] = 20,
) -> None:
    """
    List models.
    """
    require_auth()

    async def fetch():
        async with ApiClient() as client:
            if mine:
                return await client.get("/models/me", params={"limit": limit})
            elif derived_from:
                return await client.get(f"/models/{derived_from}/derived", params={"limit": limit})
            else:
                return await client.get("/models/", params={"limit": limit})

    try:
        response = asyncio.run(fetch())
        models = response if isinstance(response, list) else response.get("items", [])

        if not models:
            print_info("No models found")
            return

        table = Table(title="Models", show_edge=False)
        table.add_column("ID", style="table.id", no_wrap=True)
        table.add_column("Name", style="bold")
        table.add_column("Privacy")
        table.add_column("Parent Model")

        for model in models:
            parent = model.get("parent_model_id") or "-"
            table.add_row(
                model["id"],
                model["name"],
                model.get("privacy", "-"),
                parent,
            )

        console.print(table)

    except ApiError as e:
        print_error(f"Failed to list models: {e.message}")
        raise typer.Exit(1)


def _display_model_info(model: dict) -> None:
    """
    Display model information.
    """
    console.print(f"\n[heading]Model: {model['name']}[/heading]\n")
    console.print(f"[label]ID:[/label] {model['id']}")
    console.print(f"[label]Privacy:[/label] {model.get('privacy', '-')}")
    console.print(f"[label]Owner:[/label] {model.get('user_id', '-')}")
    console.print(f"[label]Created:[/label] {model.get('created_at', '-')}")

    if model.get("updated_at"):
        console.print(f"[label]Updated:[/label] {model['updated_at']}")

    if model.get("parent_model_id"):
        console.print(f"[label]Parent Model:[/label] {model['parent_model_id']}")

    if model.get("model_metadata"):
        console.print("\n[label]Metadata:[/label]")
        console.print_json(data=model["model_metadata"])


@app.command("info")
def info(
    model_id: Annotated[
        str,
        typer.Argument(help="Model ID (UUID or <user>/<name>)"),
    ],
) -> None:
    """
    Get detailed information about a model.
    """
    require_auth()

    async def fetch():
        async with ApiClient() as client:
            return await resolve_model_identifier(client, model_id)

    try:
        model = asyncio.run(fetch())
        _display_model_info(model)

    except ApiError as e:
        if e.status_code == 404:
            print_error(f"Model '{model_id}' not found")
        else:
            print_error(f"Failed to get model info: {e.message}")
        raise typer.Exit(1)


@app.command("update")
def update(
    model_id: Annotated[
        str,
        typer.Argument(help="Model ID (UUID or <user>/<name>) to update"),
    ],
    name: Annotated[
        str | None,
        typer.Option("--name", "-n", help="New model name"),
    ] = None,
    privacy: Annotated[
        PrivacyLevel | None,
        typer.Option("--privacy", "-p", help="New privacy level"),
    ] = None,
    metadata: Annotated[
        str | None,
        typer.Option("--metadata", "-m", help="JSON metadata string"),
    ] = None,
) -> None:
    """
    Update a model's metadata.
    """
    require_auth()

    updates: dict = {}
    if name is not None:
        updates["name"] = name
    if privacy is not None:
        updates["privacy"] = privacy.value
    if metadata is not None:
        try:
            updates["model_metadata"] = json.loads(metadata)
        except json.JSONDecodeError:
            print_error("Invalid JSON metadata")
            raise typer.Exit(1)

    if not updates:
        print_error("No updates specified. Use --name, --privacy, or --metadata.")
        raise typer.Exit(1)

    async def do_update():
        async with ApiClient() as client:
            model = await resolve_model_identifier(client, model_id)
            actual_id = model["id"]
            return await client.patch(f"/models/{actual_id}", json=updates)

    try:
        model = asyncio.run(do_update())
        print_success("Model updated successfully")
        _display_model_info(model)

    except ApiError as e:
        if e.status_code == 404:
            print_error(f"Model '{model_id}' not found")
        else:
            print_error(f"Failed to update model: {e.message}")
        raise typer.Exit(1)


@app.command("delete")
def delete(
    model_id: Annotated[
        str,
        typer.Argument(help="Model ID (UUID or <user>/<name>) to delete"),
    ],
    force: Annotated[
        bool,
        typer.Option("--force", "-f", help="Skip confirmation"),
    ] = False,
) -> None:
    """
    Delete a model.
    """
    require_auth()

    if not force:
        confirm = typer.confirm(f"Are you sure you want to delete model {model_id}?")
        if not confirm:
            print_info("Cancelled")
            return

    async def do_delete():
        async with ApiClient() as client:
            # Resolve identifier to get the actual ID
            model = await resolve_model_identifier(client, model_id)
            actual_id = model["id"]
            await client.delete(f"/models/{actual_id}")

    try:
        asyncio.run(do_delete())
        print_success("Model deleted successfully")

    except ApiError as e:
        if e.status_code == 404:
            print_error(f"Model '{model_id}' not found")
        else:
            print_error(f"Failed to delete model: {e.message}")
        raise typer.Exit(1)
