"""Tests for `trc dataset add-episodes` and add_episodes_to_dataset orchestration.

Uses unittest.mock to patch the ApiClient and the reused upload primitives
(upload_resource, abort_upload) so the orchestration flow can be verified without
real network or S3 access.
"""

from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
from typer.testing import CliRunner

from trossen_cloud_cli.api_client import ApiError
from trossen_cloud_cli.cli import app
from trossen_cloud_cli.upload import UploadError, add_episodes_to_dataset

runner = CliRunner()

MOCK_TOKEN = "tr_test_token_1234567890abcdef"
DATASET_ID = "11111111-1111-1111-1111-111111111111"


def _write_episodes(tmp_path: Path, names: list[str]) -> Path:
    """Create files under a directory; return the directory path."""
    for name in names:
        (tmp_path / name).write_bytes(b"\x00" * 16)
    return tmp_path


def _reopen_response() -> dict:
    """A minimal DatasetCreateResponse-shaped reopen response."""
    return {
        "dataset_id": DATASET_ID,
        "status": "reopened",
        "upload_urls": [
            {"file_path": "episode_000042.mcap", "direct_upload_url": "https://s3/put"}
        ],
    }


def _edit_in_progress_error() -> ApiError:
    return ApiError(
        409,
        {"code": "edit_in_progress", "message": "An edit is already in progress"},
        {"detail": {"code": "edit_in_progress", "message": "An edit is already in progress"}},
    )


def _path_exists_error() -> ApiError:
    return ApiError(
        409,
        {"code": "path_exists", "message": "Path already exists: episode_000007.mcap"},
        {
            "detail": {
                "code": "path_exists",
                "message": "Path already exists: episode_000007.mcap",
            }
        },
    )


# -- Orchestration tests (add_episodes_to_dataset) --


@pytest.mark.asyncio
async def test_orchestration_call_order(tmp_path):
    """reopen -> upload_resource(prefetched) -> finalize, with the .mcap file list."""
    _write_episodes(tmp_path, ["episode_000042.mcap"])

    post_calls = []

    async def mock_post(path, json=None):
        post_calls.append((path, json))
        if path.endswith("/episodes/reopen"):
            return _reopen_response()
        return {}

    with (
        patch("trossen_cloud_cli.api_client.get_token", return_value=MOCK_TOKEN),
        patch("trossen_cloud_cli.api_client.ApiClient.post", side_effect=mock_post),
        patch("trossen_cloud_cli.upload.upload_resource", new=AsyncMock()) as up,
    ):
        resp = await add_episodes_to_dataset(DATASET_ID, tmp_path, show_progress=False)

    assert resp["dataset_id"] == DATASET_ID
    reopen = next(c for c in post_calls if c[0].endswith("/episodes/reopen"))
    assert reopen[1]["files"] == [
        {
            "path": "episode_000042.mcap",
            "size_bytes": 16,
            "content_type": "application/octet-stream",
        }
    ]
    # upload_resource received the prefetched urls from reopen
    assert up.call_args.kwargs["prefetched_urls"] == _reopen_response()["upload_urls"]
    # finalize called after upload
    assert any(c[0].endswith("/finalize") for c in post_calls)


@pytest.mark.asyncio
async def test_mcap_filter(tmp_path):
    """A dir with mixed files reopens with only the .mcap paths."""
    _write_episodes(tmp_path, ["episode_000042.mcap", "notes.txt", "readme.md"])

    reopen_bodies = []

    async def mock_post(path, json=None):
        if path.endswith("/episodes/reopen"):
            reopen_bodies.append(json)
            return _reopen_response()
        return {}

    with (
        patch("trossen_cloud_cli.api_client.get_token", return_value=MOCK_TOKEN),
        patch("trossen_cloud_cli.api_client.ApiClient.post", side_effect=mock_post),
        patch("trossen_cloud_cli.upload.upload_resource", new=AsyncMock()),
    ):
        await add_episodes_to_dataset(DATASET_ID, tmp_path, show_progress=False)

    paths = [f["path"] for f in reopen_bodies[0]["files"]]
    assert paths == ["episode_000042.mcap"]


@pytest.mark.asyncio
async def test_no_mcap_files(tmp_path):
    """A dir with no .mcap files raises before any reopen."""
    _write_episodes(tmp_path, ["notes.txt"])

    post = AsyncMock()
    with (
        patch("trossen_cloud_cli.api_client.get_token", return_value=MOCK_TOKEN),
        patch("trossen_cloud_cli.api_client.ApiClient.post", new=post),
    ):
        with pytest.raises(UploadError, match="No .mcap episode files found"):
            await add_episodes_to_dataset(DATASET_ID, tmp_path, show_progress=False)
    post.assert_not_called()


@pytest.mark.asyncio
async def test_cancel_triggers_abort(tmp_path):
    """KeyboardInterrupt during upload aborts and re-raises."""
    _write_episodes(tmp_path, ["episode_000042.mcap"])

    async def mock_post(path, json=None):
        if path.endswith("/episodes/reopen"):
            return _reopen_response()
        return {}

    with (
        patch("trossen_cloud_cli.api_client.get_token", return_value=MOCK_TOKEN),
        patch("trossen_cloud_cli.api_client.ApiClient.post", side_effect=mock_post),
        patch(
            "trossen_cloud_cli.upload.upload_resource",
            new=AsyncMock(side_effect=KeyboardInterrupt()),
        ),
        patch("trossen_cloud_cli.upload.abort_upload", new=AsyncMock()) as abort,
    ):
        with pytest.raises(KeyboardInterrupt):
            await add_episodes_to_dataset(DATASET_ID, tmp_path, show_progress=False)

    abort.assert_awaited_once()
    assert abort.call_args.args[1] == DATASET_ID


@pytest.mark.asyncio
async def test_edit_in_progress_recovery(tmp_path):
    """First reopen 409 edit_in_progress -> abort -> retry succeeds."""
    _write_episodes(tmp_path, ["episode_000042.mcap"])

    calls = {"reopen": 0}

    async def mock_post(path, json=None):
        if path.endswith("/episodes/reopen"):
            calls["reopen"] += 1
            if calls["reopen"] == 1:
                raise _edit_in_progress_error()
            return _reopen_response()
        return {}

    with (
        patch("trossen_cloud_cli.api_client.get_token", return_value=MOCK_TOKEN),
        patch("trossen_cloud_cli.api_client.ApiClient.post", side_effect=mock_post),
        patch("trossen_cloud_cli.upload.upload_resource", new=AsyncMock()),
        patch("trossen_cloud_cli.upload.abort_upload", new=AsyncMock()) as abort,
    ):
        await add_episodes_to_dataset(
            DATASET_ID, tmp_path, show_progress=False, on_edit_in_progress=lambda: True
        )

    assert calls["reopen"] == 2
    abort.assert_awaited_once()


@pytest.mark.asyncio
async def test_edit_in_progress_declined(tmp_path):
    """Declining recovery surfaces the reopen error and does not retry."""
    _write_episodes(tmp_path, ["episode_000042.mcap"])

    async def mock_post(path, json=None):
        if path.endswith("/episodes/reopen"):
            raise _edit_in_progress_error()
        return {}

    with (
        patch("trossen_cloud_cli.api_client.get_token", return_value=MOCK_TOKEN),
        patch("trossen_cloud_cli.api_client.ApiClient.post", side_effect=mock_post),
        patch("trossen_cloud_cli.upload.upload_resource", new=AsyncMock()) as up,
    ):
        with pytest.raises(UploadError, match="already in progress"):
            await add_episodes_to_dataset(
                DATASET_ID, tmp_path, show_progress=False, on_edit_in_progress=lambda: False
            )
    up.assert_not_called()


@pytest.mark.asyncio
async def test_path_exists_surfaced(tmp_path):
    """reopen 409 path_exists surfaces its message and skips upload."""
    _write_episodes(tmp_path, ["episode_000007.mcap"])

    async def mock_post(path, json=None):
        if path.endswith("/episodes/reopen"):
            raise _path_exists_error()
        return {}

    with (
        patch("trossen_cloud_cli.api_client.get_token", return_value=MOCK_TOKEN),
        patch("trossen_cloud_cli.api_client.ApiClient.post", side_effect=mock_post),
        patch("trossen_cloud_cli.upload.upload_resource", new=AsyncMock()) as up,
    ):
        with pytest.raises(UploadError, match="Path already exists"):
            await add_episodes_to_dataset(DATASET_ID, tmp_path, show_progress=False)
    up.assert_not_called()


@pytest.mark.asyncio
async def test_not_editable_surfaced(tmp_path):
    """reopen 400 (plain-string detail) surfaces its message and skips upload."""
    _write_episodes(tmp_path, ["episode_000042.mcap"])

    async def mock_post(path, json=None):
        if path.endswith("/episodes/reopen"):
            raise ApiError(400, "Dataset is not editable")
        return {}

    with (
        patch("trossen_cloud_cli.api_client.get_token", return_value=MOCK_TOKEN),
        patch("trossen_cloud_cli.api_client.ApiClient.post", side_effect=mock_post),
        patch("trossen_cloud_cli.upload.upload_resource", new=AsyncMock()) as up,
    ):
        with pytest.raises(UploadError, match="Dataset is not editable"):
            await add_episodes_to_dataset(DATASET_ID, tmp_path, show_progress=False)
    up.assert_not_called()


# -- Command-level tests (trc dataset add-episodes) --


def test_command_help():
    result = runner.invoke(app, ["dataset", "add-episodes", "--help"])
    assert result.exit_code == 0
    assert "episodes" in result.stdout.lower()


def test_command_validation_declined(tmp_path):
    """Validation warnings without --force, declined -> exit 0, no reopen."""
    _write_episodes(tmp_path, ["episode_000042.mcap"])
    post = AsyncMock()
    with (
        patch("trossen_cloud_cli.auth.get_token", return_value=MOCK_TOKEN),
        patch(
            "trossen_cloud_cli.commands.datasets.validate_dataset",
            return_value=["bad name"],
        ),
        patch("trossen_cloud_cli.api_client.ApiClient.post", new=post),
    ):
        result = runner.invoke(
            app, ["dataset", "add-episodes", DATASET_ID, str(tmp_path)], input="n\n"
        )
    assert result.exit_code == 0
    post.assert_not_called()


def test_command_identifier_resolution(tmp_path):
    """A <user>/<name> identifier is resolved to a UUID before reopen."""
    _write_episodes(tmp_path, ["episode_000042.mcap"])

    async def mock_get(path, params=None):
        return {"id": DATASET_ID}

    async def mock_post(path, json=None):
        if path.endswith("/episodes/reopen"):
            assert DATASET_ID in path
            return _reopen_response()
        return {}

    with (
        patch("trossen_cloud_cli.auth.get_token", return_value=MOCK_TOKEN),
        patch("trossen_cloud_cli.api_client.get_token", return_value=MOCK_TOKEN),
        patch(
            "trossen_cloud_cli.commands.datasets.validate_dataset",
            return_value=[],
        ),
        patch("trossen_cloud_cli.api_client.ApiClient.get", side_effect=mock_get),
        patch("trossen_cloud_cli.api_client.ApiClient.post", side_effect=mock_post),
        patch("trossen_cloud_cli.upload.upload_resource", new=AsyncMock()),
    ):
        result = runner.invoke(app, ["dataset", "add-episodes", "alice/demo", str(tmp_path)])
    assert result.exit_code == 0, result.stdout
    assert DATASET_ID in result.stdout


def test_command_cancel_in_progress_flag(tmp_path):
    """--cancel-in-progress auto-cancels an in-progress edit and retries."""
    _write_episodes(tmp_path, ["episode_000042.mcap"])

    calls = {"reopen": 0}

    async def mock_get(path, params=None):
        return {"id": DATASET_ID}

    async def mock_post(path, json=None):
        if path.endswith("/episodes/reopen"):
            calls["reopen"] += 1
            if calls["reopen"] == 1:
                raise _edit_in_progress_error()
            return _reopen_response()
        return {}

    with (
        patch("trossen_cloud_cli.auth.get_token", return_value=MOCK_TOKEN),
        patch("trossen_cloud_cli.api_client.get_token", return_value=MOCK_TOKEN),
        patch("trossen_cloud_cli.commands.datasets.validate_dataset", return_value=[]),
        patch("trossen_cloud_cli.api_client.ApiClient.get", side_effect=mock_get),
        patch("trossen_cloud_cli.api_client.ApiClient.post", side_effect=mock_post),
        patch("trossen_cloud_cli.upload.upload_resource", new=AsyncMock()),
        patch("trossen_cloud_cli.upload.abort_upload", new=AsyncMock()) as abort,
    ):
        result = runner.invoke(
            app,
            ["dataset", "add-episodes", DATASET_ID, str(tmp_path), "--cancel-in-progress"],
        )
    assert result.exit_code == 0, result.stdout
    assert calls["reopen"] == 2
    abort.assert_awaited_once()


def test_command_force_does_not_cancel_in_progress(tmp_path):
    """--force alone must NOT auto-cancel an in-progress edit (declined at prompt)."""
    _write_episodes(tmp_path, ["episode_000042.mcap"])

    async def mock_get(path, params=None):
        return {"id": DATASET_ID}

    async def mock_post(path, json=None):
        if path.endswith("/episodes/reopen"):
            raise _edit_in_progress_error()
        return {}

    with (
        patch("trossen_cloud_cli.auth.get_token", return_value=MOCK_TOKEN),
        patch("trossen_cloud_cli.api_client.get_token", return_value=MOCK_TOKEN),
        patch("trossen_cloud_cli.commands.datasets.validate_dataset", return_value=[]),
        patch("trossen_cloud_cli.api_client.ApiClient.get", side_effect=mock_get),
        patch("trossen_cloud_cli.api_client.ApiClient.post", side_effect=mock_post),
        patch("trossen_cloud_cli.upload.upload_resource", new=AsyncMock()),
        patch("trossen_cloud_cli.upload.abort_upload", new=AsyncMock()) as abort,
    ):
        # --force skips the validation prompt; the edit-in-progress prompt is
        # declined via "n", so no abort happens.
        result = runner.invoke(
            app,
            ["dataset", "add-episodes", DATASET_ID, str(tmp_path), "--force"],
            input="n\n",
        )
    assert result.exit_code == 1
    abort.assert_not_called()
