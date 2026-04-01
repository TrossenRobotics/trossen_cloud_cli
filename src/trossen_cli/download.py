"""Parallel download engine."""

import asyncio
from pathlib import Path

import httpx

from .api_client import ApiClient, ApiError
from .config import get_config
from .output import format_size, print_success
from .progress import TransferProgress


class DownloadError(Exception):
    """
    Download error.
    """

    pass


def _print_download_summary(file_count: int, total_bytes: int, elapsed: float) -> None:
    """
    Print a one-line download summary.
    """
    if elapsed > 0:
        speed = total_bytes / elapsed
        mins, secs = divmod(int(elapsed), 60)
        time_str = f"{mins}m {secs}s" if mins else f"{secs}s"
        print_success(
            f"Downloaded {file_count} files ({format_size(total_bytes)}) "
            f"in {time_str} ({format_size(speed)}/s)"
        )
    else:
        print_success(f"Downloaded {file_count} files ({format_size(total_bytes)})")


async def download_file(
    download_client: httpx.AsyncClient,
    url: str,
    local_path: Path,
    filename: str,
    progress: TransferProgress | None = None,
    chunk_size: int = 65536,
) -> None:
    """
    Download a single file from a presigned URL.
    """
    # Ensure parent directory exists
    local_path.parent.mkdir(parents=True, exist_ok=True)

    # Prevent symlink attacks
    if local_path.is_symlink():
        raise DownloadError(f"Refusing to write to symlink: {local_path}")

    # Stream download
    async with download_client.stream("GET", url) as response:
        response.raise_for_status()

        # Get content length for progress
        total_size = int(response.headers.get("Content-Length", 0))

        if progress and total_size > 0:
            progress.add_file(filename, total_size)

        with open(local_path, "wb") as f:
            async for chunk in response.aiter_bytes(chunk_size=chunk_size):
                f.write(chunk)
                if progress:
                    progress.advance_file(filename, len(chunk))

    if progress:
        progress.complete_file(filename)


async def download_resource(
    resource_id: str,
    resource_type: str,
    output_dir: Path,
    show_progress: bool = True,
) -> None:
    """
    Download all files for a resource.
    """
    config = get_config()

    async with ApiClient() as client:
        # Get download URLs
        try:
            response = await client.get(f"/{resource_type}/{resource_id}/download-urls")
        except ApiError as e:
            raise DownloadError(f"Failed to get download URLs: {e.message}")

        raw_files = response.get("files", [])
        if not raw_files:
            raise DownloadError("No files to download")

        # Transform FileDownloadInfo array to path->url mapping
        files: dict[str, str] = {f["path"]: f["download_url"] for f in raw_files}

        output_dir.mkdir(parents=True, exist_ok=True)
        resolved_output = output_dir.resolve()

        # Validate all file paths before downloading anything
        for file_path in files:
            if file_path.startswith("/"):
                raise DownloadError(f"Absolute path not allowed: {file_path}")
            resolved = (resolved_output / file_path).resolve()
            if not resolved.is_relative_to(resolved_output):
                raise DownloadError(
                    f"Path traversal detected, refusing to write outside "
                    f"output directory: {file_path}"
                )

        async with httpx.AsyncClient(
            timeout=httpx.Timeout(300.0, connect=30.0),
            limits=httpx.Limits(max_connections=config.download.parallel_files),
        ) as download_client:
            if show_progress:
                total_size = sum(f["size_bytes"] for f in raw_files)
                with TransferProgress(
                    "Downloading", max_visible_files=min(config.download.parallel_files, 8)
                ) as progress:
                    progress.set_total_size(total_size)
                    progress.set_total_files(len(raw_files))

                    semaphore = asyncio.Semaphore(config.download.parallel_files)

                    chunk_size = config.download.stream_chunk_size

                    async def download_with_semaphore(file_path: str, url: str):
                        async with semaphore:
                            local_path = output_dir / file_path
                            await download_file(
                                download_client, url, local_path, file_path, progress, chunk_size
                            )

                    tasks = [download_with_semaphore(path, url) for path, url in files.items()]
                    await asyncio.gather(*tasks)

                    elapsed = progress.elapsed_seconds

                _print_download_summary(len(raw_files), total_size, elapsed)
            else:
                semaphore = asyncio.Semaphore(config.download.parallel_files)
                chunk_size = config.download.stream_chunk_size

                async def download_with_semaphore(file_path: str, url: str):
                    async with semaphore:
                        local_path = output_dir / file_path
                        await download_file(download_client, url, local_path, file_path, None, chunk_size)

                tasks = [download_with_semaphore(path, url) for path, url in files.items()]
                await asyncio.gather(*tasks)


async def download_dataset(
    dataset_id: str,
    output_dir: Path,
    show_progress: bool = True,
) -> None:
    """
    Download a dataset.
    """
    await download_resource(dataset_id, "datasets", output_dir, show_progress)


async def download_model(
    model_id: str,
    output_dir: Path,
    show_progress: bool = True,
) -> None:
    """
    Download a model.
    """
    await download_resource(model_id, "models", output_dir, show_progress)
