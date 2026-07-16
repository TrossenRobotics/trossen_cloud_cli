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

from ..api_client import API_BASE_URL, ApiClient, ApiError
from ..auth import require_auth
from ..download import download_dataset
from ..output import (
    console,
    format_size,
    print_error,
    print_info,
    print_success,
    print_warning,
)
from ..types import DatasetType, PrivacyLevel
from ..upload import (
    UploadError,
    add_episodes_to_dataset,
    create_and_upload_dataset,
)
from ..validators import detect_dataset_type, validate_dataset

app = typer.Typer(help="Manage datasets")


_TYPE_ALIASES = {"lerobot": "lerobot_v3", "mcap": "trossenmcap"}


def _dataset_web_url(dataset_id: str) -> str:
    """Build the web app URL for a dataset from the configured API base URL.

    The frontend is served from the same origin as the API, so strip the ``/api/v1``
    suffix off the base URL and append the dataset detail route.

    :param dataset_id: The dataset's UUID.
    :return: The browser URL for the dataset detail page.
    """
    origin = API_BASE_URL.rstrip("/")
    if origin.endswith("/api/v1"):
        origin = origin[: -len("/api/v1")]
    return f"{origin}/datasets/{dataset_id}"


def _valid_type_names() -> str:
    """Comma-separated list of accepted --type values (canonical names + aliases)."""
    return ", ".join([*DatasetType, *_TYPE_ALIASES])


_DatasetTypeOption = Annotated[
    str | None,
    typer.Option(
        "--type",
        "-t",
        help=f"Dataset type ({_valid_type_names()}). Auto-detected if omitted.",
    ),
]


def _parse_dataset_type(value: str | None) -> DatasetType | None:
    """Parse a --type string into a DatasetType, resolving aliases (case-insensitive)."""
    if value is None:
        return None
    lower = value.lower()
    resolved = _TYPE_ALIASES.get(lower, lower)
    try:
        return DatasetType(resolved)
    except ValueError:
        raise typer.BadParameter(
            f"Invalid dataset type '{value}'. Valid: {_valid_type_names()}"
        ) from None


def _resolve_dataset_type(path: Path, dataset_type: DatasetType | None) -> DatasetType:
    """Auto-detect dataset type if not provided, or exit with an error."""
    if dataset_type is not None:
        return dataset_type
    detected = detect_dataset_type(path)
    if detected is None:
        print_error(
            f"Could not detect dataset type. Use --type to specify ({_valid_type_names()})."
        )
        raise typer.Exit(1)
    print_info(f"Detected dataset type: {detected.value}")
    return detected


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


async def _fetch_all_episodes(client: ApiClient, dataset_id: str) -> list[dict]:
    """
    Page through the episodes endpoint and return every episode.

    The list endpoint caps ``limit`` at 200 per request, so a dataset with more
    than 200 episodes cannot be listed in a single call. This pages through with
    ``offset`` until the full list is collected. Callers that resolve filenames
    to episode ids must see the complete list, or episodes past the first page
    would be silently mis-reported as missing.

    :param client: The authenticated API client.
    :param dataset_id: The resolved dataset UUID.
    :return: All episode dicts across every page.

    """
    items: list[dict] = []
    offset = 0
    while True:
        page = await client.get(
            f"/datasets/{dataset_id}/episodes", params={"limit": 200, "offset": offset}
        )
        batch = page.get("items", [])
        items.extend(batch)
        total = page.get("total")
        if len(batch) < 200 or (total is not None and len(items) >= total):
            break
        offset += 200
    return items


def _canonical_episode_key(name: str) -> str:
    """
    Canonical full key for matching, preserving any directory prefix.

    Normalizes Windows ``\\`` to POSIX ``/``, drops leading ``./`` and ``/``
    segments, and strips a trailing ``.mcap`` (case-insensitive). Directories
    are kept so ``a/episode_000001.mcap`` and ``b/episode_000001.mcap`` remain
    distinct keys.

    :param name: An episode filename, path, or bare basename.
    :return: The normalized full key (e.g. ``a/episode_000001``).

    """
    s = name.replace("\\", "/")
    while s.startswith("./"):
        s = s[2:]
    s = s.lstrip("/")
    return s[:-5] if s.lower().endswith(".mcap") else s


def _episode_basename(name: str) -> str:
    """
    Last path segment of the canonical key, used as an unambiguous fallback.

    :param name: An episode filename, path, or bare basename.
    :return: The final ``episode_NNNNNN`` segment.

    """
    return _canonical_episode_key(name).rsplit("/", 1)[-1]


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
    dataset_type_str: _DatasetTypeOption = None,
    privacy: Annotated[
        PrivacyLevel,
        typer.Option("--privacy", "-p", help="Privacy level"),
    ] = PrivacyLevel.PRIVATE,
    metadata: Annotated[
        str | None,
        typer.Option("--metadata", "-m", help="JSON metadata string"),
    ] = None,
    force: Annotated[
        bool,
        typer.Option("--force", "-f", help="Skip validation confirmation prompt"),
    ] = False,
) -> None:
    """
    Upload a dataset to Trossen Cloud.
    """
    parsed_type = _parse_dataset_type(dataset_type_str)
    require_auth()
    dataset_type = _resolve_dataset_type(path, parsed_type)

    # Parse metadata if provided
    metadata_dict = None
    if metadata:
        try:
            metadata_dict = json.loads(metadata)
        except json.JSONDecodeError:
            print_error("Invalid JSON metadata")
            raise typer.Exit(1)

    # Validate dataset before upload
    validation_warnings = validate_dataset(path, dataset_type)
    if validation_warnings:
        console.print(
            f"\n[warning]Found {len(validation_warnings)} validation warning(s):[/warning]"
        )
        for w in validation_warnings:
            print_warning(w)
        console.print()
        if not force and not typer.confirm("Continue with upload?"):
            raise typer.Exit(0)

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
        console.print(f"[bold]URL:[/bold]  {_dataset_web_url(dataset['id'])}")

    except KeyboardInterrupt:
        raise typer.Exit(1)
    except UploadError as e:
        print_error(str(e))
        raise typer.Exit(1)


@app.command("add-episodes")
def add_episodes(
    dataset_id: Annotated[
        str,
        typer.Argument(help="Dataset ID (UUID or <user>/<name>)"),
    ],
    path: Annotated[
        Path,
        typer.Argument(
            help="Path to the .mcap file or directory of new episodes",
            exists=True,
            resolve_path=True,
        ),
    ],
    force: Annotated[
        bool,
        typer.Option("--force", "-f", help="Skip the validation confirmation prompt"),
    ] = False,
    cancel_in_progress: Annotated[
        bool,
        typer.Option(
            "--cancel-in-progress",
            help="If another edit is already in progress on this dataset, cancel it and retry "
            "(this discards that edit — it may belong to another process or session)",
        ),
    ] = False,
) -> None:
    """
    Add new episodes to an existing trossenmcap dataset.
    """
    require_auth()

    # Validate the new files as trossenmcap (warn-only, same posture as `upload`).
    # This sees only the new files, so a duplicate episode number is caught
    # server-side (reopen 409 path_exists), not here.
    validation_warnings = validate_dataset(path, DatasetType.TROSSENMCAP)
    if validation_warnings:
        console.print(
            f"\n[warning]Found {len(validation_warnings)} validation warning(s):[/warning]"
        )
        for w in validation_warnings:
            print_warning(w)
        console.print()
        if not force and not typer.confirm("Continue adding episodes?"):
            raise typer.Exit(0)

    def _confirm_abort() -> bool:
        return cancel_in_progress or typer.confirm(
            "An edit is already in progress. Cancel it and retry?"
        )

    async def _run() -> str:
        async with ApiClient() as client:
            actual_id = (await resolve_dataset_identifier(client, dataset_id))["id"]
        await add_episodes_to_dataset(actual_id, path, on_edit_in_progress=_confirm_abort)
        return actual_id

    try:
        actual_id = asyncio.run(_run())
        console.print(f"[bold]ID:[/bold] {actual_id}")
    except KeyboardInterrupt:
        raise typer.Exit(1)
    except UploadError as e:
        print_error(str(e))
        raise typer.Exit(1)
    except ApiError as e:
        print_error(e.message if isinstance(e.message, str) else "Failed to add episodes")
        raise typer.Exit(1)


@app.command("episodes")
def episodes(
    dataset_id: Annotated[
        str,
        typer.Argument(help="Dataset ID (UUID or <user>/<name>)"),
    ],
) -> None:
    """
    List episodes in a dataset.
    """
    require_auth()

    async def fetch() -> list[dict]:
        async with ApiClient() as client:
            actual_id = (await resolve_dataset_identifier(client, dataset_id))["id"]
            return await _fetch_all_episodes(client, actual_id)

    try:
        items = asyncio.run(fetch())
    except ApiError as e:
        print_error(e.message if isinstance(e.message, str) else "Failed to list episodes")
        raise typer.Exit(1)

    if not items:
        print_info("No episodes found")
        return

    table = Table(title=f"Episodes ({len(items)})", show_edge=False)
    table.add_column("#", justify="right")
    table.add_column("Episode", style="bold")
    table.add_column("Size", justify="right")
    table.add_column("Duration")
    table.add_column("Viz")
    # Position is derived from sort order (display only); episodes are matched by
    # filename, never by this index.
    for i, ep in enumerate(items):
        dur = ep.get("duration_seconds")
        viz = ep.get("viz")
        table.add_row(
            str(i),
            ep["source_key"],
            format_size(ep.get("source_size_bytes") or 0),
            f"{dur:.1f}s" if dur is not None else "-",
            viz.get("status", "-") if isinstance(viz, dict) else "-",
        )
    console.print(table)


@app.command("remove-episodes")
def remove_episodes(
    dataset_id: Annotated[
        str,
        typer.Argument(help="Dataset ID (UUID or <user>/<name>)"),
    ],
    episodes: Annotated[
        list[str],
        typer.Argument(help="Episode filename(s), e.g. episode_000042.mcap"),
    ],
    force: Annotated[
        bool,
        typer.Option("--force", "-f", help="Skip the removal confirmation"),
    ] = False,
) -> None:
    """
    Remove episodes from a trossenmcap dataset. This permanently deletes episode data.
    """
    require_auth()

    async def _run() -> dict:
        async with ApiClient() as client:
            actual_id = (await resolve_dataset_identifier(client, dataset_id))["id"]
            all_eps = await _fetch_all_episodes(client, actual_id)

            # Full-key map is unambiguous; basename map is the fallback but may
            # collapse episodes that share a name across subdirectories.
            by_key = {_canonical_episode_key(ep["source_key"]): ep["id"] for ep in all_eps}
            by_basename: dict[str, list[str]] = {}
            for ep in all_eps:
                by_basename.setdefault(_episode_basename(ep["source_key"]), []).append(ep["id"])

            # Resolve each requested name: exact full-key match wins; otherwise
            # fall back to basename, but refuse (never guess) when a bare name
            # maps to more than one episode. Dedupe, preserving first-seen order.
            matched_ids: list[str] = []
            for orig in episodes:
                key = _canonical_episode_key(orig)
                if key in by_key:
                    ep_id = by_key[key]
                else:
                    ids = by_basename.get(_episode_basename(orig), [])
                    if len(ids) > 1:
                        print_warning(
                            f"'{orig}' matches {len(ids)} episodes in different "
                            "subdirectories — specify the full path to disambiguate; skipping"
                        )
                        continue
                    if not ids:
                        print_warning(f"No episode matching '{orig}' — skipping")
                        continue
                    ep_id = ids[0]
                if ep_id not in matched_ids:
                    matched_ids.append(ep_id)

            if not matched_ids:
                raise UploadError("No matching episodes found to remove")

            if len(matched_ids) > 200:
                raise UploadError(
                    f"{len(matched_ids)} episodes exceeds the 200-per-call limit; "
                    "remove them in smaller batches"
                )

            if not force:
                console.print(
                    f"[warning]This permanently removes {len(matched_ids)} episode(s) "
                    "and cannot be undone.[/warning]"
                )
                if not typer.confirm("Remove these episodes?"):
                    raise typer.Exit(0)

            return await client.post(
                f"/datasets/{actual_id}/episodes/remove",
                json={"episode_ids": matched_ids},
            )

    try:
        resp = asyncio.run(_run())
    except typer.Exit:
        raise
    except UploadError as e:
        print_error(str(e))
        raise typer.Exit(1)
    except KeyboardInterrupt:
        raise typer.Exit(1)
    except ApiError as e:
        print_error(e.message if isinstance(e.message, str) else "Failed to remove episodes")
        raise typer.Exit(1)

    print_success(f"Removed {len(resp.get('removed', []))} episode(s)")
    if resp.get("not_found"):
        # Server-side not_found (e.g. concurrent removal) — distinct from the
        # client-side unresolved names warned above.
        print_warning(f"{len(resp['not_found'])} episode(s) were already gone")
    console.print(
        f"[label]Remaining:[/label] {resp.get('file_count', '?')} files, "
        f"{format_size(resp.get('total_size_bytes') or 0)}"
    )


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
    dataset_type_str: _DatasetTypeOption = None,
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
    force: Annotated[
        bool,
        typer.Option("--force", "-f", help="Skip validation confirmation prompt"),
    ] = False,
) -> None:
    """
    Import a public HuggingFace dataset into Trossen Cloud.

    Downloads the dataset from HuggingFace Hub, then uploads it.
    """
    from huggingface_hub import snapshot_download
    from huggingface_hub.utils import HfHubHTTPError, RepositoryNotFoundError

    parsed_type = _parse_dataset_type(dataset_type_str)
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

        dataset_type = _resolve_dataset_type(local_path, parsed_type)

        # Validate dataset before upload
        validation_warnings = validate_dataset(local_path, dataset_type)
        if validation_warnings:
            console.print(
                f"\n[warning]Found {len(validation_warnings)} validation warning(s):[/warning]"
            )
            for w in validation_warnings:
                print_warning(w)
            console.print()
            if not force and not typer.confirm("Continue with upload?"):
                raise typer.Exit(0)

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
        console.print(f"[bold]URL:[/bold]  {_dataset_web_url(dataset['id'])}")

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
