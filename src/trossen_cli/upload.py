"""Multipart upload engine."""

import asyncio
import mimetypes
from pathlib import Path
from typing import Any

import httpx

from .api_client import ApiClient, ApiError
from .config import get_config, get_uploads_dir
from .output import format_size, print_error, print_info, print_success, print_warning
from .progress import TransferProgress
from .types import FileInfo, FileUploadState, UploadState


class UploadError(Exception):
    """
    Upload error.
    """

    pass


def get_content_type(file_path: Path) -> str:
    """
    Get the MIME type for a file based on its extension.

    Uses Python's mimetypes module to guess the content type from the file
    extension. Falls back to 'application/octet-stream' if the type cannot
    be determined.

    :param file_path: Path to the file to determine the MIME type for.
    :return: The MIME type string (e.g., 'image/png', 'application/json').

    """
    mime_type, _ = mimetypes.guess_type(str(file_path))
    return mime_type or "application/octet-stream"


def collect_files(local_path: Path) -> list[FileInfo]:
    """
    Collect all files from a path for upload.

    If the path is a single file, returns a list containing just that file's
    information. If the path is a directory, recursively collects all files
    within the directory tree.

    :param local_path: Path to a file or directory to collect files from.
    :return: List of FileInfo objects containing path, size, and content type
        for each file found.

    """
    files: list[FileInfo] = []

    if local_path.is_file():
        if not local_path.name.startswith("."):
            files.append(
                FileInfo(
                    path=local_path.name,
                    size_bytes=local_path.stat().st_size,
                    content_type=get_content_type(local_path),
                )
            )
    else:
        for file_path in local_path.rglob("*"):
            if file_path.is_file():
                relative_path = file_path.relative_to(local_path)
                # Skip hidden files and files inside hidden directories
                if any(part.startswith(".") for part in relative_path.parts):
                    continue
                files.append(
                    FileInfo(
                        path=str(relative_path),
                        size_bytes=file_path.stat().st_size,
                        content_type=get_content_type(file_path),
                    )
                )

    return files


def save_upload_state(state: UploadState) -> None:
    """
    Save upload state to disk for resume support.

    Persists the current upload state to a JSON file in the uploads directory.
    This allows uploads to be resumed if interrupted.

    :param state: The upload state object to persist.

    """
    state_file = get_uploads_dir() / f"{state.resource_id}.json"
    with open(state_file, "w") as f:
        f.write(state.model_dump_json(indent=2))


def load_upload_state(resource_id: str) -> UploadState | None:
    """
    Load a previously saved upload state from disk.

    Attempts to load and validate an upload state file for the given resource.
    Used to resume interrupted uploads.

    :param resource_id: The unique identifier of the resource being uploaded.
    :return: The loaded upload state if found and valid, None otherwise.

    """
    state_file = get_uploads_dir() / f"{resource_id}.json"
    if state_file.exists():
        try:
            with open(state_file) as f:
                return UploadState.model_validate_json(f.read())
        except Exception:
            pass
    return None


def clear_upload_state(resource_id: str) -> None:
    """
    Remove the upload state file for a resource.

    Called after a successful upload completes or when an upload is aborted
    to clean up the state file.

    :param resource_id: The unique identifier of the resource.

    """
    state_file = get_uploads_dir() / f"{resource_id}.json"
    if state_file.exists():
        state_file.unlink()


UPLOAD_MAX_RETRIES = 5
STREAM_CHUNK_SIZE = 256 * 1024  # 256 KB chunks for streaming progress
BATCH_CHUNK_SIZE = 500  # Max files per batch API call
STATE_SAVE_INTERVAL = 10  # Save state every N part completions


async def upload_part(
    upload_client: httpx.AsyncClient,
    upload_url: str,
    file_path: Path,
    part_number: int,
    part_size: int,
    file_size: int,
    progress: TransferProgress | None,
    filename: str,
) -> str:
    """
    Upload a single part of a multipart upload with retries.

    Streams data in chunks to provide real-time progress updates.

    :param upload_client: Shared async HTTP client for storage requests.
    :param upload_url: The presigned URL for uploading this part.
    :param file_path: Local path to the file being uploaded.
    :param part_number: The 1-based part number for this chunk.
    :param part_size: The size of each part in bytes.
    :param file_size: The total size of the file in bytes.
    :param progress: Optional progress tracker to update after upload.
    :param filename: The display name for the file in progress output.
    :return: The ETag returned by storage for this part.
    :raises httpx.ConnectError: If all retries are exhausted.

    """
    offset = (part_number - 1) * part_size
    chunk_size = min(part_size, file_size - offset)

    def _read_data() -> bytes:
        with open(file_path, "rb") as f:
            f.seek(offset)
            return f.read(chunk_size)

    last_error: Exception | None = None
    for attempt in range(UPLOAD_MAX_RETRIES):
        data = _read_data()
        bytes_sent_this_attempt = 0

        async def _streaming_body():
            """
            Async generator that yields chunks and tracks bytes sent.
            """
            nonlocal bytes_sent_this_attempt
            sent = 0
            while sent < len(data):
                end = min(sent + STREAM_CHUNK_SIZE, len(data))
                yield data[sent:end]
                chunk_len = end - sent
                if progress:
                    progress.advance_file(filename, chunk_len)
                bytes_sent_this_attempt += chunk_len
                sent = end

        try:
            response = await upload_client.put(
                upload_url,
                content=_streaming_body(),
                headers={"Content-Length": str(len(data))},
            )
            response.raise_for_status()
            return response.headers.get("ETag", "")
        except (httpx.ConnectError, httpx.ConnectTimeout, httpx.HTTPStatusError) as e:
            if isinstance(e, httpx.HTTPStatusError) and e.response.status_code < 500:
                raise
            last_error = e
            # Undo only the bytes actually sent in this attempt
            if progress and bytes_sent_this_attempt > 0:
                progress.advance_file(filename, -bytes_sent_this_attempt)
            if attempt < UPLOAD_MAX_RETRIES - 1:
                await asyncio.sleep((2**attempt) * 0.5)

    raise last_error  # type: ignore[misc]


async def _upload_file_parts(
    upload_client: httpx.AsyncClient,
    file_path: str,
    local_path: Path,
    part_urls: dict[int, str],
    part_size: int,
    progress: TransferProgress | None = None,
    state: UploadState | None = None,
) -> None:
    """
    Upload all parts of a single file in parallel.

    :param upload_client: Shared async HTTP client for storage requests.
    :param file_path: The relative path of the file within the resource.
    :param local_path: The local filesystem path to the file.
    :param part_urls: Mapping of part_number -> presigned URL.
    :param part_size: Size of each part in bytes.
    :param progress: Optional progress tracker for UI updates.
    :param state: Optional upload state for resume support.

    """
    config = get_config()
    file_size = local_path.stat().st_size
    semaphore = asyncio.Semaphore(config.upload.parallel_parts)

    # Filter out parts that have already been uploaded (resume support)
    parts_completed_set = set()
    if state and file_path in state.files:
        parts_completed_set = set(state.files[file_path].parts_completed)

    parts_to_upload = [pn for pn in part_urls if pn not in parts_completed_set]

    # Account for already-uploaded bytes in progress tracking
    if progress and parts_completed_set:
        # Calculate bytes from completed parts
        completed_bytes = 0
        for part_num in parts_completed_set:
            offset = (part_num - 1) * part_size
            chunk_size = min(part_size, file_size - offset)
            completed_bytes += chunk_size
        # Advance progress to reflect already-completed work
        progress.advance_file(file_path, completed_bytes)

    parts_since_save = 0

    async def do_part(part_number: int) -> None:
        nonlocal parts_since_save
        async with semaphore:
            await upload_part(
                upload_client,
                part_urls[part_number],
                local_path,
                part_number,
                part_size,
                file_size,
                progress,
                file_path,
            )
            if state and file_path in state.files:
                # Avoid duplicate entries
                if part_number not in parts_completed_set:
                    state.files[file_path].parts_completed.append(part_number)
                    parts_completed_set.add(part_number)
                parts_since_save += 1
                if parts_since_save >= STATE_SAVE_INTERVAL:
                    save_upload_state(state)
                    parts_since_save = 0

    await asyncio.gather(*[do_part(pn) for pn in parts_to_upload])

    # Final save for any remaining parts
    if state and parts_since_save > 0:
        save_upload_state(state)


def _print_transfer_summary(verb: str, file_count: int, total_bytes: int, elapsed: float) -> None:
    """
    Print a one-line transfer summary.
    """
    if elapsed > 0:
        speed = total_bytes / elapsed
        mins, secs = divmod(int(elapsed), 60)
        time_str = f"{mins}m {secs}s" if mins else f"{secs}s"
        print_success(
            f"{verb} {file_count} files ({format_size(total_bytes)}) "
            f"in {time_str} ({format_size(speed)}/s)"
        )
    else:
        print_success(f"{verb} {file_count} files ({format_size(total_bytes)})")


async def upload_resource(
    client: ApiClient,
    resource_id: str,
    resource_type: str,
    local_path: Path,
    files: list[FileInfo],
    show_progress: bool = True,
    prefetched_urls: list[dict] | None = None,
) -> None:
    """
    Upload all files for a resource using batch initiate/complete.

    Flow: 1. Use prefetched URLs from create, or batch initiate files
       (chunked into groups of BATCH_CHUNK_SIZE to stay under API payload limits)
    2. Upload parts in parallel across all files
    3. Batch complete files (chunked the same way)

    :param client: The authenticated API client.
    :param resource_id: The ID of the resource being uploaded to.
    :param resource_type: The type of resource ('datasets' or 'models').
    :param local_path: The local base path for the files.
    :param files: List of file information objects to upload.
    :param show_progress: Whether to display a progress bar. Defaults to True.
    :param prefetched_urls: Optional pre-fetched upload URLs from the create response, skipping the
        batch initiate step.

    """
    config = get_config()
    base_path = local_path if local_path.is_dir() else local_path.parent

    # Load or create state
    state = load_upload_state(resource_id)
    if state is None:
        state = UploadState(
            resource_id=resource_id,
            resource_type=resource_type,
            local_path=str(local_path),
        )
        save_upload_state(state)

    # Filter out already-completed files
    files_to_upload = []
    for f in files:
        if state and f.path in state.files and state.files[f.path].status == "complete":
            continue
        files_to_upload.append(f)

    # Step 1: Get upload URLs in chunks (avoid API payload limits)
    direct_urls: dict[str, str] = {}
    multipart_info: dict[str, tuple[dict[int, str], int]] = {}

    def _parse_batch_response(batch_files: list[dict]) -> None:
        for file_result in batch_files:
            fp = file_result["file_path"]
            if file_result.get("error"):
                continue
            if file_result.get("direct_upload_url"):
                direct_urls[fp] = file_result["direct_upload_url"]
            else:
                part_urls = {}
                for p in file_result.get("part_urls") or []:
                    part_urls[p["part_number"]] = p["upload_url"]
                if part_urls:
                    multipart_info[fp] = (part_urls, file_result["part_size_bytes"])
            if state:
                if fp not in state.files:
                    state.files[fp] = FileUploadState(status="uploading")
                else:
                    state.files[fp].status = "uploading"

    if prefetched_urls:
        _parse_batch_response(prefetched_urls)
    else:
        # Chunk batch-initiate calls to stay under API payload limits
        for i in range(0, len(files_to_upload), BATCH_CHUNK_SIZE):
            chunk = files_to_upload[i : i + BATCH_CHUNK_SIZE]
            batch_init = await client.post(
                f"/{resource_type}/{resource_id}/files/initiate",
                json={
                    "file_paths": [f.path for f in chunk],
                    "include_part_urls": True,
                },
            )
            _parse_batch_response(batch_init.get("files", []))

    save_upload_state(state)

    # Step 2: Upload all files in parallel with error isolation
    file_sem = asyncio.Semaphore(config.upload.parallel_files)
    failed_files: list[str] = []

    async with httpx.AsyncClient(
        timeout=httpx.Timeout(300.0),
        limits=httpx.Limits(
            max_connections=config.upload.parallel_files * config.upload.parallel_parts
        ),
    ) as upload_client:

        async def upload_one_file(
            file_info: FileInfo,
            progress: TransferProgress | None,
        ) -> None:
            async with file_sem:
                fp = file_info.path
                actual_path = local_path if local_path.is_file() else base_path / fp

                if progress:
                    progress.add_file(fp, file_info.size_bytes)

                if fp not in direct_urls and fp not in multipart_info:
                    failed_files.append(fp)
                    if progress:
                        progress.complete_file(fp)
                    return

                try:
                    if fp in direct_urls:
                        await upload_part(
                            upload_client,
                            direct_urls[fp],
                            actual_path,
                            1,
                            file_info.size_bytes,
                            file_info.size_bytes,
                            progress,
                            fp,
                        )
                    else:
                        part_urls_map, part_size = multipart_info[fp]
                        await _upload_file_parts(
                            upload_client,
                            fp,
                            actual_path,
                            part_urls_map,
                            part_size,
                            progress,
                            state,
                        )
                except Exception as e:
                    failed_files.append(fp)
                    print_error(f"Failed to upload {fp}: {e}")
                    if progress:
                        progress.complete_file(fp)
                    return

                if progress:
                    progress.complete_file(fp)

                if state and fp in state.files:
                    state.files[fp].status = "complete"

        if show_progress:
            with TransferProgress(
                "Uploading", max_visible_files=min(config.upload.parallel_files, 8)
            ) as progress:
                upload_size = sum(f.size_bytes for f in files_to_upload)
                progress.set_total_size(upload_size)
                progress.set_total_files(len(files_to_upload))

                await asyncio.gather(*[upload_one_file(f, progress) for f in files_to_upload])
                elapsed = progress.elapsed_seconds

            _print_transfer_summary("Uploaded", len(files_to_upload), upload_size, elapsed)
        else:
            await asyncio.gather(*[upload_one_file(f, None) for f in files_to_upload])

    save_upload_state(state)

    if failed_files:
        raise UploadError(f"{len(failed_files)} file(s) failed to upload")

    # Step 3: Batch complete all files (chunked)
    completed_paths = [
        f.path
        for f in files_to_upload
        if f.path in direct_urls or f.path in multipart_info
        if f.path not in failed_files
    ]

    for i in range(0, len(completed_paths), BATCH_CHUNK_SIZE):
        paths_chunk = completed_paths[i : i + BATCH_CHUNK_SIZE]
        await client.post(
            f"/{resource_type}/{resource_id}/files/complete",
            json={"file_paths": paths_chunk},
        )

    clear_upload_state(resource_id)


async def abort_upload(
    client: ApiClient,
    resource_id: str,
    resource_type: str,
) -> None:
    """
    Abort an in-progress upload and clean up resources.

    Notifies the API to abort the upload (which cleans up incomplete
    multipart uploads) and removes the local upload state file.

    Silently ignores API errors since the upload may already be aborted
    or the resource may not exist.

    :param client: The authenticated API client.
    :param resource_id: The ID of the resource being uploaded.
    :param resource_type: The type of resource ('datasets' or 'models').

    """
    try:
        await client.post(f"/{resource_type}/{resource_id}/abort")
        print_success("Upload cancelled successfully")
    except ApiError:
        print_warning(
            "Could not confirm cancellation with server. "
            "Check your dashboard to verify the upload was cancelled."
        )

    clear_upload_state(resource_id)


async def create_and_upload_dataset(
    name: str,
    local_path: Path,
    dataset_type: str,
    privacy: str,
    metadata: dict[str, Any] | None = None,
    show_progress: bool = True,
) -> dict[str, Any]:
    """
    Create a new dataset and upload all files from a local path.

    Performs the complete dataset creation workflow:
    1. Collects all files from the local path
    2. Creates the dataset resource via API
    3. Uploads all files using multipart upload
    4. Finalizes the dataset

    If upload fails, automatically aborts the upload and cleans up.

    :param name: The name for the new dataset.
    :param local_path: Path to the file or directory to upload.
    :param dataset_type: The type of dataset (e.g., 'lerobot').
    :param privacy: Privacy setting ('public' or 'private').
    :param metadata: Optional additional metadata for the dataset.
    :param show_progress: Whether to display upload progress. Defaults to True.
    :return: The created dataset information from the API.
    :raises UploadError: If no files are found or dataset creation fails.
    :raises ApiError: If API requests fail during upload.

    """
    # Collect files
    files = collect_files(local_path)
    if not files:
        raise UploadError("No files found to upload")

    async with ApiClient() as client:
        # Create dataset with upload URLs in one call
        create_data: dict[str, Any] = {
            "name": name,
            "type": dataset_type,
            "privacy": privacy,
            "files": [f.model_dump() for f in files],
        }
        if metadata:
            create_data["dataset_metadata"] = metadata

        try:
            dataset = await client.post("/datasets", json=create_data)
        except ApiError as e:
            raise UploadError(e.message)

        resource_id = dataset["dataset_id"]
        dataset["id"] = resource_id

        try:
            # Upload files (pass prefetched URLs from create response)
            await upload_resource(
                client,
                resource_id,
                "datasets",
                local_path,
                files,
                show_progress,
                prefetched_urls=dataset.get("upload_urls"),
            )

            # Finalize
            try:
                await client.post(f"/datasets/{resource_id}/finalize")
            except ApiError as e:
                if "already finalized" not in e.message.lower():
                    raise

            return dataset

        except (Exception, KeyboardInterrupt, asyncio.CancelledError) as e:
            is_cancelled = isinstance(e, (KeyboardInterrupt, asyncio.CancelledError))
            if is_cancelled:
                print_info("Stopping upload...")
            else:
                print_error(f"Upload failed: {e}")
                print_info("Aborting upload...")
            await abort_upload(client, resource_id, "datasets")
            raise


async def create_and_upload_model(
    name: str,
    local_path: Path,
    privacy: str,
    base_model_id: str | None = None,
    metadata: dict[str, Any] | None = None,
    show_progress: bool = True,
) -> dict[str, Any]:
    """
    Create a new model and upload all files from a local path.

    Performs the complete model creation workflow:
    1. Collects all files from the local path
    2. Creates the model resource via API
    3. Uploads all files using multipart upload
    4. Finalizes the model

    If upload fails, automatically aborts the upload and cleans up.

    :param name: The name for the new model.
    :param local_path: Path to the file or directory to upload.
    :param privacy: Privacy setting ('public' or 'private').
    :param base_model_id: Optional ID of a base model this derives from.
    :param metadata: Optional additional metadata for the model.
    :param show_progress: Whether to display upload progress. Defaults to True.
    :return: The created model information from the API.
    :raises UploadError: If no files are found or model creation fails.
    :raises ApiError: If API requests fail during upload.

    """
    # Collect files
    files = collect_files(local_path)
    if not files:
        raise UploadError("No files found to upload")

    async with ApiClient() as client:
        # Create model
        create_data: dict[str, Any] = {
            "name": name,
            "privacy": privacy,
            "files": [f.model_dump() for f in files],
        }
        if base_model_id:
            create_data["parent_model_id"] = base_model_id
        if metadata:
            create_data["model_metadata"] = metadata

        try:
            model = await client.post("/models", json=create_data)
        except ApiError as e:
            raise UploadError(e.message)

        resource_id = model["model_id"]
        model["id"] = resource_id

        try:
            # Upload files
            await upload_resource(
                client,
                resource_id,
                "models",
                local_path,
                files,
                show_progress,
            )

            # Finalize
            try:
                await client.post(f"/models/{resource_id}/finalize")
            except ApiError as e:
                if "already finalized" not in e.message.lower():
                    raise

            return model

        except (Exception, KeyboardInterrupt, asyncio.CancelledError) as e:
            is_cancelled = isinstance(e, (KeyboardInterrupt, asyncio.CancelledError))
            if is_cancelled:
                print_info("Stopping upload...")
            else:
                print_error(f"Upload failed: {e}")
                print_info("Aborting upload...")
            await abort_upload(client, resource_id, "models")
            raise
