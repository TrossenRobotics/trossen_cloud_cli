"""Dataset commands."""

import asyncio
import json
import re
import shutil
import tempfile
from pathlib import Path
from typing import Annotated

import typer
from rich.table import Table

from ..api_client import ApiClient, ApiError
from ..auth import require_auth
from ..download import download_dataset
from ..output import console, print_error, print_info, print_success
from ..types import DatasetType, PrivacyLevel
from ..upload import UploadError, create_and_upload_dataset

app = typer.Typer(help="Manage datasets")


def is_user_name_format(identifier: str) -> bool:
    """
    Check if identifier is in <user>/<name> format.
    """
    return "/" in identifier and not identifier.startswith("/")


async def resolve_dataset_identifier(client: ApiClient, identifier: str) -> dict:
    """
    Resolve a dataset identifier (UUID or user/name) to dataset info.
    """
    if is_user_name_format(identifier):
        # user/name format
        return await client.get(f"/datasets/{identifier}")
    else:
        # UUID format
        return await client.get(f"/datasets/{identifier}")


@app.command("upload")
def upload(
    path: Annotated[
        Path,
        typer.Argument(
            help="Path to the dataset directory or file to upload",
            exists=True,
            resolve_path=True,
        ),
    ],
    name: Annotated[
        str,
        typer.Option("--name", "-n", help="Dataset name"),
    ],
    dataset_type: Annotated[
        DatasetType,
        typer.Option("--type", "-t", help="Dataset type"),
    ],
    privacy: Annotated[
        PrivacyLevel,
        typer.Option("--privacy", "-p", help="Privacy level"),
    ] = PrivacyLevel.PRIVATE,
    metadata: Annotated[
        str | None,
        typer.Option("--metadata", "-m", help="JSON metadata string"),
    ] = None,
) -> None:
    """
    Upload a dataset to Trossen Cloud.
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
        dataset = asyncio.run(
            create_and_upload_dataset(
                name=name,
                local_path=path,
                dataset_type=dataset_type.value,
                privacy=privacy.value,
                metadata=metadata_dict,
            )
        )
        console.print(f"[bold]ID:[/bold]   {dataset['id']}")
        console.print(f"[bold]Name:[/bold] {name}")

    except KeyboardInterrupt:
        raise typer.Exit(1)
    except UploadError as e:
        print_error(str(e))
        raise typer.Exit(1)


def _parse_hf_repo_id(repo_id_or_url: str) -> str:
    """
    Extract a HuggingFace repo ID from a URL or return as-is if already an ID.

    Accepts:
      - https://huggingface.co/datasets/org/name
      - org/name
    """
    match = re.match(r"https?://huggingface\.co/datasets/([^/]+/[^/]+?)(?:/.*)?$", repo_id_or_url)
    if match:
        return match.group(1)
    if "/" in repo_id_or_url and not repo_id_or_url.startswith("http"):
        return repo_id_or_url
    raise typer.BadParameter(
        f"Invalid HuggingFace dataset: '{repo_id_or_url}'. "
        "Use a URL (https://huggingface.co/datasets/org/name) or repo ID (org/name)."
    )


@app.command("import-hf")
def import_hf(
    repo: Annotated[
        str,
        typer.Argument(
            help="HuggingFace dataset URL or repo ID (e.g., org/dataset-name)",
        ),
    ],
    name: Annotated[
        str | None,
        typer.Option("--name", "-n", help="Dataset name (defaults to HF repo name)"),
    ] = None,
    dataset_type: Annotated[
        DatasetType,
        typer.Option("--type", "-t", help="Dataset type"),
    ] = DatasetType.LEROBOT,
    privacy: Annotated[
        PrivacyLevel,
        typer.Option("--privacy", "-p", help="Privacy level"),
    ] = PrivacyLevel.PRIVATE,
    metadata: Annotated[
        str | None,
        typer.Option("--metadata", "-m", help="JSON metadata string"),
    ] = None,
    revision: Annotated[
        str | None,
        typer.Option("--revision", "-r", help="Git revision (branch, tag, or commit)"),
    ] = None,
    keep_local: Annotated[
        bool,
        typer.Option("--keep-local", help="Keep the downloaded files after upload"),
    ] = False,
) -> None:
    """
    Import a public HuggingFace dataset into Trossen Cloud.

    Downloads the dataset from HuggingFace Hub, then uploads it.
    """
    from huggingface_hub import snapshot_download
    from huggingface_hub.utils import HfHubHTTPError, RepositoryNotFoundError

    require_auth()

    repo_id = _parse_hf_repo_id(repo)
    dataset_name = name or repo_id.split("/")[-1]

    # Parse metadata
    metadata_dict = None
    if metadata:
        try:
            metadata_dict = json.loads(metadata)
        except json.JSONDecodeError:
            print_error("Invalid JSON metadata")
            raise typer.Exit(1)

    # Add HF source info to metadata
    hf_meta = {"huggingface_repo": repo_id}
    if revision:
        hf_meta["huggingface_revision"] = revision
    if metadata_dict:
        metadata_dict = {**hf_meta, **metadata_dict}
    else:
        metadata_dict = hf_meta

    # Download from HuggingFace
    tmp_dir = None
    try:
        print_info(f"Downloading from HuggingFace: {repo_id}")

        tmp_dir = tempfile.mkdtemp(prefix="trc_hf_")
        local_path = Path(
            snapshot_download(
                repo_id=repo_id,
                repo_type="dataset",
                revision=revision,
                local_dir=str(Path(tmp_dir) / dataset_name),
            )
        )

        print_success(f"Downloaded to {local_path}")

        # Upload to Trossen Cloud
        dataset = asyncio.run(
            create_and_upload_dataset(
                name=dataset_name,
                local_path=local_path,
                dataset_type=dataset_type.value,
                privacy=privacy.value,
                metadata=metadata_dict,
            )
        )
        console.print(f"[bold]ID:[/bold]   {dataset['id']}")
        console.print(f"[bold]Name:[/bold] {dataset_name}")

    except RepositoryNotFoundError:
        print_error(f"HuggingFace dataset '{repo_id}' not found")
        raise typer.Exit(1)
    except HfHubHTTPError as e:
        print_error(f"HuggingFace download failed: {e}")
        raise typer.Exit(1)
    except KeyboardInterrupt:
        raise typer.Exit(1)
    except UploadError as e:
        print_error(str(e))
        raise typer.Exit(1)
    finally:
        if tmp_dir and not keep_local:
            shutil.rmtree(tmp_dir, ignore_errors=True)
        elif tmp_dir:
            print_info(f"Local copy kept at: {tmp_dir}")


@app.command("download")
def download(
    dataset_id: Annotated[
        str,
        typer.Argument(help="Dataset ID (UUID or <user>/<name>) to download"),
    ],
    output_dir: Annotated[
        Path,
        typer.Argument(help="Output directory"),
    ],
) -> None:
    """
    Download a dataset from Trossen Cloud.
    """
    require_auth()

    async def do_download():
        async with ApiClient() as client:
            # Resolve identifier to get the actual ID
            dataset = await resolve_dataset_identifier(client, dataset_id)
            actual_id = dataset["id"]
        # Download using the resolved ID
        await download_dataset(actual_id, output_dir)

    try:
        asyncio.run(do_download())
        console.print(f"[bold]Path:[/bold] {output_dir}")

    except KeyboardInterrupt:
        print_error("Download interrupted")
        raise typer.Exit(1)
    except ApiError as e:
        if e.status_code == 404:
            print_error(f"Dataset '{dataset_id}' not found")
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
        typer.Argument(help="Dataset path in <user>/<name> format (e.g., trossen/aloha-demo)"),
    ],
) -> None:
    """
    View a dataset by user/name path.
    """
    require_auth()

    if not is_user_name_format(path):
        print_error("Invalid format. Use <user>/<name> (e.g., trossen/aloha-demo)")
        raise typer.Exit(1)

    async def fetch():
        async with ApiClient() as client:
            return await resolve_dataset_identifier(client, path)

    try:
        dataset = asyncio.run(fetch())
        _display_dataset_info(dataset)

    except ApiError as e:
        if e.status_code == 404:
            print_error(f"Dataset '{path}' not found")
        else:
            print_error(f"Failed to get dataset: {e.message}")
        raise typer.Exit(1)


@app.command("list")
def list_datasets(
    mine: Annotated[
        bool,
        typer.Option("--mine", help="Show only your datasets"),
    ] = False,
    limit: Annotated[
        int,
        typer.Option("--limit", "-l", help="Maximum number of datasets to show"),
    ] = 20,
) -> None:
    """
    List datasets.
    """
    require_auth()

    async def fetch():
        async with ApiClient() as client:
            if mine:
                return await client.get("/datasets/me", params={"limit": limit})
            else:
                return await client.get("/datasets/", params={"limit": limit})

    try:
        response = asyncio.run(fetch())
        datasets = response if isinstance(response, list) else response.get("items", [])

        if not datasets:
            print_info("No datasets found")
            return

        table = Table(title="Datasets", show_edge=False)
        table.add_column("ID", style="table.id", no_wrap=True)
        table.add_column("Name", style="bold")
        table.add_column("Type")
        table.add_column("Privacy")

        for ds in datasets:
            table.add_row(
                ds["id"],
                ds["name"],
                ds.get("type", "-"),
                ds.get("privacy", "-"),
            )

        console.print(table)

    except ApiError as e:
        print_error(f"Failed to list datasets: {e.message}")
        raise typer.Exit(1)


def _display_dataset_info(dataset: dict) -> None:
    """
    Display dataset information.
    """
    console.print(f"\n[heading]Dataset: {dataset['name']}[/heading]\n")
    console.print(f"[label]ID:[/label] {dataset['id']}")
    console.print(f"[label]Type:[/label] {dataset.get('type', '-')}")
    console.print(f"[label]Privacy:[/label] {dataset.get('privacy', '-')}")
    console.print(f"[label]Owner:[/label] {dataset.get('user_id', '-')}")
    console.print(f"[label]Created:[/label] {dataset.get('created_at', '-')}")

    if dataset.get("updated_at"):
        console.print(f"[label]Updated:[/label] {dataset['updated_at']}")

    if dataset.get("dataset_metadata"):
        console.print("\n[label]Metadata:[/label]")
        console.print_json(data=dataset["dataset_metadata"])


@app.command("info")
def info(
    dataset_id: Annotated[
        str,
        typer.Argument(help="Dataset ID (UUID or <user>/<name>)"),
    ],
) -> None:
    """
    Get detailed information about a dataset.
    """
    require_auth()

    async def fetch():
        async with ApiClient() as client:
            return await resolve_dataset_identifier(client, dataset_id)

    try:
        dataset = asyncio.run(fetch())
        _display_dataset_info(dataset)

    except ApiError as e:
        if e.status_code == 404:
            print_error(f"Dataset '{dataset_id}' not found")
        else:
            print_error(f"Failed to get dataset info: {e.message}")
        raise typer.Exit(1)


@app.command("update")
def update(
    dataset_id: Annotated[
        str,
        typer.Argument(help="Dataset ID (UUID or <user>/<name>) to update"),
    ],
    name: Annotated[
        str | None,
        typer.Option("--name", "-n", help="New dataset name"),
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
    Update a dataset's metadata.
    """
    require_auth()

    updates: dict = {}
    if name is not None:
        updates["name"] = name
    if privacy is not None:
        updates["privacy"] = privacy.value
    if metadata is not None:
        try:
            updates["dataset_metadata"] = json.loads(metadata)
        except json.JSONDecodeError:
            print_error("Invalid JSON metadata")
            raise typer.Exit(1)

    if not updates:
        print_error("No updates specified. Use --name, --privacy, or --metadata.")
        raise typer.Exit(1)

    async def do_update():
        async with ApiClient() as client:
            dataset = await resolve_dataset_identifier(client, dataset_id)
            actual_id = dataset["id"]
            return await client.patch(f"/datasets/{actual_id}", json=updates)

    try:
        dataset = asyncio.run(do_update())
        print_success("Dataset updated successfully")
        _display_dataset_info(dataset)

    except ApiError as e:
        if e.status_code == 404:
            print_error(f"Dataset '{dataset_id}' not found")
        else:
            print_error(f"Failed to update dataset: {e.message}")
        raise typer.Exit(1)


@app.command("delete")
def delete(
    dataset_id: Annotated[
        str,
        typer.Argument(help="Dataset ID (UUID or <user>/<name>) to delete"),
    ],
    force: Annotated[
        bool,
        typer.Option("--force", "-f", help="Skip confirmation"),
    ] = False,
) -> None:
    """
    Delete a dataset.
    """
    require_auth()

    if not force:
        confirm = typer.confirm(f"Are you sure you want to delete dataset {dataset_id}?")
        if not confirm:
            print_info("Cancelled")
            return

    async def do_delete():
        async with ApiClient() as client:
            # Resolve identifier to get the actual ID
            dataset = await resolve_dataset_identifier(client, dataset_id)
            actual_id = dataset["id"]
            await client.delete(f"/datasets/{actual_id}")

    try:
        asyncio.run(do_delete())
        print_success("Dataset deleted successfully")

    except ApiError as e:
        if e.status_code == 404:
            print_error(f"Dataset '{dataset_id}' not found")
        else:
            print_error(f"Failed to delete dataset: {e.message}")
        raise typer.Exit(1)
