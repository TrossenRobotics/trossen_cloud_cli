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


def _write_inline_file(local_path: Path, content: str) -> None:
    """
    Write a file whose contents were inlined in the download-urls response.

    The backend inlines small JSON artifacts it rewrites at serve time (e.g. PEFT
    adapter configs with a co-located base path); for those, ``download_url`` is
    null and the rendered body comes back in ``content``.
    """
    local_path.parent.mkdir(parents=True, exist_ok=True)
    if local_path.is_symlink():
        raise DownloadError(f"Refusing to write to symlink: {local_path}")
    local_path.write_text(content)


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

        output_dir.mkdir(parents=True, exist_ok=True)
        resolved_output = output_dir.resolve()

        # Validate all file paths before downloading anything
        for f in raw_files:
            file_path = f["path"]
            if file_path.startswith("/"):
                raise DownloadError(f"Absolute path not allowed: {file_path}")
            resolved = (resolved_output / file_path).resolve()
            if not resolved.is_relative_to(resolved_output):
                raise DownloadError(
                    f"Path traversal detected, refusing to write outside "
                    f"output directory: {file_path}"
                )

        # Partition by transport. Inline files carry their bytes in the response
        # body (no S3 round-trip); URL files stream from a presigned S3 URL.
        inline_files = [f for f in raw_files if f.get("content") is not None]
        url_files = [f for f in raw_files if f.get("content") is None]
        missing_url = [f["path"] for f in url_files if not f.get("download_url")]
        if missing_url:
            raise DownloadError(
                f"Response has no content and no download_url for: {missing_url}"
            )

        # Inline files are small JSON; write them up front rather than spinning up
        # a download worker per file. Count them in the summary so totals match the
        # full asset.
        for f in inline_files:
            _write_inline_file(output_dir / f["path"], f["content"])

        total_size = sum(f["size_bytes"] for f in raw_files)
        url_size = sum(f["size_bytes"] for f in url_files)

        if not url_files:
            if show_progress:
                _print_download_summary(len(raw_files), total_size, 0.0)
            return

        async with httpx.AsyncClient(
            timeout=httpx.Timeout(300.0, connect=30.0),
            limits=httpx.Limits(
                max_connections=config.download.parallel_files,
                max_keepalive_connections=config.download.parallel_files,
            ),
        ) as download_client:
            if show_progress:
                with TransferProgress(
                    "Downloading", max_visible_files=min(config.download.parallel_files, 8)
                ) as progress:
                    # Count inline files in the progress total so the live bar matches
                    # the final summary. Inline files are already on disk by this
                    # point; register-then-complete each one to bump the byte counter
                    # and file counter without showing them in the active list.
                    progress.set_total_size(total_size)
                    progress.set_total_files(len(raw_files))
                    for f in inline_files:
                        progress.add_file(f["path"], f["size_bytes"])
                        progress.complete_file(f["path"])

                    semaphore = asyncio.Semaphore(config.download.parallel_files)

                    chunk_size = config.download.stream_chunk_size

                    async def download_with_semaphore(file_path: str, url: str):
                        async with semaphore:
                            local_path = output_dir / file_path
                            await download_file(
                                download_client, url, local_path, file_path, progress, chunk_size
                            )

                    tasks = [
                        download_with_semaphore(f["path"], f["download_url"]) for f in url_files
                    ]
                    await asyncio.gather(*tasks)

                    elapsed = progress.elapsed_seconds

                _print_download_summary(len(raw_files), total_size, elapsed)
            else:
                semaphore = asyncio.Semaphore(config.download.parallel_files)
                chunk_size = config.download.stream_chunk_size

                async def download_with_semaphore(file_path: str, url: str):
                    async with semaphore:
                        local_path = output_dir / file_path
                        await download_file(
                            download_client, url, local_path, file_path, None, chunk_size
                        )

                tasks = [
                    download_with_semaphore(f["path"], f["download_url"]) for f in url_files
                ]
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
