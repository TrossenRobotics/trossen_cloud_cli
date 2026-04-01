"""Tests verifying CLI commands call the correct API endpoints with correct payloads.

Uses unittest.mock to patch ApiClient methods and verify endpoint paths,
HTTP methods, and request payloads match the OpenAPI spec.
"""

from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
from typer.testing import CliRunner

from trossen_cli.cli import app

runner = CliRunner()

# Common mock token for auth
MOCK_TOKEN = "tr_test_token_1234567890abcdef"


def mock_auth():
    """
    Patch auth to return a mock token.
    """
    return patch("trossen_cli.auth.get_token", return_value=MOCK_TOKEN)


def mock_client_get(return_value):
    """
    Patch ApiClient.get to return a mock response.
    """
    mock = AsyncMock(return_value=return_value)
    return patch("trossen_cli.api_client.ApiClient.get", mock), mock


def mock_client_post(return_value):
    """
    Patch ApiClient.post to return a mock response.
    """
    mock = AsyncMock(return_value=return_value)
    return patch("trossen_cli.api_client.ApiClient.post", mock), mock


def mock_client_patch(return_value):
    """
    Patch ApiClient.patch to return a mock response.
    """
    mock = AsyncMock(return_value=return_value)
    return patch("trossen_cli.api_client.ApiClient.patch", mock), mock


def mock_client_delete(return_value=None):
    """
    Patch ApiClient.delete to return a mock response.
    """
    mock = AsyncMock(return_value=return_value or {})
    return patch("trossen_cli.api_client.ApiClient.delete", mock), mock


# -- Dataset List Tests --


class TestDatasetList:
    def test_list_calls_datasets_endpoint(self):
        """
        GET /datasets/ with limit param.
        """
        datasets = [{"id": "abc-123", "name": "test-ds", "type": "raw", "privacy": "private"}]
        get_patch, get_mock = mock_client_get(datasets)
        with mock_auth(), get_patch:
            result = runner.invoke(app, ["dataset", "list"])
        assert result.exit_code == 0
        get_mock.assert_called_once_with("/datasets/", params={"limit": 20})

    def test_list_mine_calls_me_endpoint(self):
        """
        GET /datasets/me when --mine is used.
        """
        get_patch, get_mock = mock_client_get([])
        with mock_auth(), get_patch:
            result = runner.invoke(app, ["dataset", "list", "--mine"])
        assert result.exit_code == 0
        get_mock.assert_called_once_with("/datasets/me", params={"limit": 20})

    def test_list_with_limit(self):
        """
        GET /datasets/ passes limit param.
        """
        get_patch, get_mock = mock_client_get([])
        with mock_auth(), get_patch:
            result = runner.invoke(app, ["dataset", "list", "--limit", "5"])
        assert result.exit_code == 0
        get_mock.assert_called_once_with("/datasets/", params={"limit": 5})


# -- Dataset Info Tests --


class TestDatasetInfo:
    def test_info_calls_get_with_uuid(self):
        """
        GET /datasets/{uuid}.
        """
        dataset = {
            "id": "abc-123",
            "name": "test-ds",
            "type": "raw",
            "privacy": "private",
            "user_id": "user-1",
        }
        get_patch, get_mock = mock_client_get(dataset)
        with mock_auth(), get_patch:
            result = runner.invoke(app, ["dataset", "info", "abc-123"])
        assert result.exit_code == 0
        get_mock.assert_called_once_with("/datasets/abc-123")
        assert "test-ds" in result.stdout

    def test_info_displays_user_id_not_owner_id(self):
        """
        Display should show user_id field from API.
        """
        dataset = {
            "id": "abc-123",
            "name": "test-ds",
            "type": "raw",
            "privacy": "private",
            "user_id": "user-456",
        }
        get_patch, _ = mock_client_get(dataset)
        with mock_auth(), get_patch:
            result = runner.invoke(app, ["dataset", "info", "abc-123"])
        assert "user-456" in result.stdout

    def test_info_displays_dataset_metadata(self):
        """
        Display should use dataset_metadata field.
        """
        dataset = {
            "id": "abc-123",
            "name": "test-ds",
            "type": "raw",
            "privacy": "private",
            "dataset_metadata": {"key": "value"},
        }
        get_patch, _ = mock_client_get(dataset)
        with mock_auth(), get_patch:
            result = runner.invoke(app, ["dataset", "info", "abc-123"])
        assert "Metadata" in result.stdout


# -- Dataset Delete Tests --


class TestDatasetDelete:
    def test_delete_calls_correct_endpoint(self):
        """
        DELETE /datasets/{uuid}.
        """
        dataset = {"id": "abc-123", "name": "test-ds", "type": "raw", "privacy": "private"}
        get_patch, _ = mock_client_get(dataset)
        del_patch, del_mock = mock_client_delete()
        with mock_auth(), get_patch, del_patch:
            result = runner.invoke(app, ["dataset", "delete", "abc-123", "--force"])
        assert result.exit_code == 0
        del_mock.assert_called_once_with("/datasets/abc-123")


# -- Dataset Update Tests --


class TestDatasetUpdate:
    def test_update_calls_patch_endpoint(self):
        """
        PATCH /datasets/{uuid} with update payload.
        """
        dataset = {
            "id": "abc-123",
            "name": "new-name",
            "type": "raw",
            "privacy": "public",
        }
        get_patch, _ = mock_client_get(dataset)
        patch_ctx, patch_mock = mock_client_patch(dataset)
        with mock_auth(), get_patch, patch_ctx:
            result = runner.invoke(
                app, ["dataset", "update", "abc-123", "--name", "new-name", "--privacy", "public"]
            )
        assert result.exit_code == 0
        patch_mock.assert_called_once_with(
            "/datasets/abc-123", json={"name": "new-name", "privacy": "public"}
        )

    def test_update_with_metadata(self):
        """
        PATCH /datasets/{uuid} sends dataset_metadata.
        """
        dataset = {"id": "abc-123", "name": "ds", "type": "raw", "privacy": "private"}
        get_patch, _ = mock_client_get(dataset)
        patch_ctx, patch_mock = mock_client_patch(dataset)
        with mock_auth(), get_patch, patch_ctx:
            result = runner.invoke(
                app, ["dataset", "update", "abc-123", "--metadata", '{"env": "lab"}']
            )
        assert result.exit_code == 0
        call_args = patch_mock.call_args
        assert call_args[1]["json"]["dataset_metadata"] == {"env": "lab"}

    def test_update_no_options_errors(self):
        """
        Update with no options should fail.
        """
        with mock_auth():
            result = runner.invoke(app, ["dataset", "update", "abc-123"])
        assert result.exit_code == 1


# -- Model List Tests --


class TestModelList:
    def test_list_calls_models_endpoint(self):
        """
        GET /models/ with limit param.
        """
        models = [{"id": "m-123", "name": "test-model", "privacy": "private"}]
        get_patch, get_mock = mock_client_get(models)
        with mock_auth(), get_patch:
            result = runner.invoke(app, ["model", "list"])
        assert result.exit_code == 0
        get_mock.assert_called_once_with("/models/", params={"limit": 20})

    def test_list_mine_calls_me_endpoint(self):
        """
        GET /models/me when --mine is used.
        """
        get_patch, get_mock = mock_client_get([])
        with mock_auth(), get_patch:
            result = runner.invoke(app, ["model", "list", "--mine"])
        assert result.exit_code == 0
        get_mock.assert_called_once_with("/models/me", params={"limit": 20})

    def test_list_derived_from_calls_derived_endpoint(self):
        """
        GET /models/{parent_id}/derived when --derived-from is used.
        """
        get_patch, get_mock = mock_client_get([])
        with mock_auth(), get_patch:
            result = runner.invoke(app, ["model", "list", "--derived-from", "parent-uuid"])
        assert result.exit_code == 0
        get_mock.assert_called_once_with("/models/parent-uuid/derived", params={"limit": 20})


# -- Model Info Tests --


class TestModelInfo:
    def test_info_displays_parent_model_id(self):
        """
        Display should show parent_model_id, not base_model_id.
        """
        model = {
            "id": "m-123",
            "name": "test-model",
            "privacy": "public",
            "parent_model_id": "parent-789",
            "user_id": "user-1",
        }
        get_patch, _ = mock_client_get(model)
        with mock_auth(), get_patch:
            result = runner.invoke(app, ["model", "info", "m-123"])
        assert "parent-789" in result.stdout
        assert "Parent Model" in result.stdout


# -- Model Update Tests --


class TestModelUpdate:
    def test_update_calls_patch_endpoint(self):
        """
        PATCH /models/{uuid} with update payload.
        """
        model = {"id": "m-123", "name": "new-name", "privacy": "public"}
        get_patch, _ = mock_client_get(model)
        patch_ctx, patch_mock = mock_client_patch(model)
        with mock_auth(), get_patch, patch_ctx:
            result = runner.invoke(app, ["model", "update", "m-123", "--name", "new-name"])
        assert result.exit_code == 0
        patch_mock.assert_called_once_with("/models/m-123", json={"name": "new-name"})

    def test_update_sends_model_metadata(self):
        """
        PATCH /models/{uuid} sends model_metadata (not metadata).
        """
        model = {"id": "m-123", "name": "m", "privacy": "private"}
        get_patch, _ = mock_client_get(model)
        patch_ctx, patch_mock = mock_client_patch(model)
        with mock_auth(), get_patch, patch_ctx:
            result = runner.invoke(
                app, ["model", "update", "m-123", "--metadata", '{"arch": "act"}']
            )
        assert result.exit_code == 0
        call_args = patch_mock.call_args
        assert call_args[1]["json"]["model_metadata"] == {"arch": "act"}


# -- Training Job Tests --


class TestTrainingJobList:
    def test_list_calls_me_endpoint(self):
        """
        GET /training-jobs/me.
        """
        jobs = [
            {
                "id": "j-123",
                "name": "job-1",
                "status": "running",
                "base_model_id": "bm-1",
                "dataset_id": "ds-1",
                "progress": 0.5,
            }
        ]
        get_patch, get_mock = mock_client_get(jobs)
        with mock_auth(), get_patch:
            result = runner.invoke(app, ["training-job", "list"])
        assert result.exit_code == 0
        get_mock.assert_called_once_with("/training-jobs/me", params={"limit": 10, "offset": 0})

    def test_list_with_status_filter(self):
        """
        GET /training-jobs/me with status param.
        """
        get_patch, get_mock = mock_client_get([])
        with mock_auth(), get_patch:
            result = runner.invoke(app, ["training-job", "list", "--status", "running"])
        assert result.exit_code == 0
        get_mock.assert_called_once_with(
            "/training-jobs/me", params={"limit": 10, "offset": 0, "status": "running"}
        )


class TestTrainingJobInfo:
    def test_info_calls_correct_endpoint(self):
        """
        GET /training-jobs/{job_id}.
        """
        job = {
            "id": "j-123",
            "name": "my-job",
            "status": "completed",
            "runner_type": "sagemaker",
            "base_model_id": "bm-1",
            "dataset_id": "ds-1",
            "hyperparameters": {"num_steps": 1000, "batch_size": 8},
        }
        get_patch, get_mock = mock_client_get(job)
        with mock_auth(), get_patch:
            result = runner.invoke(app, ["training-job", "info", "j-123"])
        assert result.exit_code == 0
        get_mock.assert_called_once_with("/training-jobs/j-123")
        assert "my-job" in result.stdout


class TestTrainingJobCreate:
    def test_create_sends_correct_payload(self):
        """
        POST /training-jobs with correct fields.
        """
        response = {"job_id": "j-new", "status": "pending", "created_at": "2024-01-01"}
        post_patch, post_mock = mock_client_post(response)
        with mock_auth(), post_patch:
            result = runner.invoke(
                app,
                [
                    "training-job",
                    "create",
                    "--name",
                    "test-job",
                    "--base-model-id",
                    "bm-1",
                    "--dataset-id",
                    "ds-1",
                    "--num-steps",
                    "1000",
                    "--batch-size",
                    "8",
                    "--learning-rate",
                    "0.001",
                    "--checkpoint-frequency",
                    "200",
                ],
            )
        assert result.exit_code == 0
        post_mock.assert_called_once()
        call_args = post_mock.call_args
        assert call_args[0][0] == "/training-jobs"
        payload = call_args[1]["json"]
        assert payload["name"] == "test-job"
        assert payload["base_model_id"] == "bm-1"
        assert payload["dataset_id"] == "ds-1"
        assert payload["instance_type"] == "gpu-medium"
        assert payload["hyperparameters"]["num_steps"] == 1000
        assert payload["hyperparameters"]["batch_size"] == 8
        assert payload["hyperparameters"]["learning_rate"] == 0.001
        assert payload["hyperparameters"]["checkpoint_frequency"] == 200


class TestTrainingJobCancel:
    def test_cancel_calls_correct_endpoint(self):
        """
        POST /training-jobs/{job_id}/cancel.
        """
        response = {"job_id": "j-123", "status": "canceled", "message": "Cancelled"}
        post_patch, post_mock = mock_client_post(response)
        with mock_auth(), post_patch:
            result = runner.invoke(app, ["training-job", "cancel", "j-123", "--force"])
        assert result.exit_code == 0
        post_mock.assert_called_once_with("/training-jobs/j-123/cancel", json=None)

    def test_cancel_with_reason(self):
        """
        POST /training-jobs/{job_id}/cancel with reason.
        """
        response = {"job_id": "j-123", "status": "canceled", "message": "Cancelled"}
        post_patch, post_mock = mock_client_post(response)
        with mock_auth(), post_patch:
            result = runner.invoke(
                app,
                ["training-job", "cancel", "j-123", "--force", "--reason", "no longer needed"],
            )
        assert result.exit_code == 0
        post_mock.assert_called_once_with(
            "/training-jobs/j-123/cancel", json={"reason": "no longer needed"}
        )


class TestTrainingJobModels:
    def test_models_calls_correct_endpoint(self):
        """
        GET /training-jobs/{job_id}/models.
        """
        models = [{"id": "m-1", "name": "checkpoint-1", "privacy": "private"}]
        get_patch, get_mock = mock_client_get(models)
        with mock_auth(), get_patch:
            result = runner.invoke(app, ["training-job", "models", "j-123"])
        assert result.exit_code == 0
        get_mock.assert_called_once_with("/training-jobs/j-123/models")


# -- Upload/Download Integration Tests --


class TestUploadEndpoints:
    @pytest.mark.asyncio
    async def test_create_dataset_sends_correct_payload(self):
        """
        POST /datasets with correct DatasetCreate fields.
        """
        from trossen_cli.upload import create_and_upload_dataset

        create_response = {"dataset_id": "ds-new", "files": [], "status": "pending"}
        finalize_response = {
            "dataset_id": "ds-new",
            "status": "complete",
            "total_size_bytes": 100,
            "file_count": 1,
            "finalized_at": "2024-01-01",
        }

        with (
            patch("trossen_cli.auth.get_token", return_value=MOCK_TOKEN),
            patch("trossen_cli.api_client.ApiClient.post", new_callable=AsyncMock) as post_mock,
            patch("trossen_cli.upload.upload_resource", new_callable=AsyncMock),
            patch("trossen_cli.upload.collect_files") as collect_mock,
        ):
            from trossen_cli.types import FileInfo

            collect_mock.return_value = [
                FileInfo(path="data.bin", size_bytes=100, content_type="application/octet-stream")
            ]
            post_mock.side_effect = [create_response, finalize_response]

            result = await create_and_upload_dataset(
                name="test-ds",
                local_path=Path("/tmp/test"),
                dataset_type="raw",
                privacy="private",
                metadata={"env": "lab"},
                show_progress=False,
            )

        # Verify create call
        create_call = post_mock.call_args_list[0]
        assert create_call[0][0] == "/datasets"
        payload = create_call[1]["json"]
        assert payload["name"] == "test-ds"
        assert payload["type"] == "raw"
        assert payload["privacy"] == "private"
        assert payload["dataset_metadata"] == {"env": "lab"}
        assert len(payload["files"]) == 1

        # Verify finalize call
        finalize_call = post_mock.call_args_list[1]
        assert finalize_call[0][0] == "/datasets/ds-new/finalize"

        # Verify return value has id
        assert result["id"] == "ds-new"

    @pytest.mark.asyncio
    async def test_create_model_sends_parent_model_id(self):
        """
        POST /models uses parent_model_id, not base_model_id.
        """
        from trossen_cli.upload import create_and_upload_model

        create_response = {"model_id": "m-new", "files": [], "status": "pending"}
        finalize_response = {
            "model_id": "m-new",
            "status": "complete",
            "total_size_bytes": 100,
            "file_count": 1,
            "finalized_at": "2024-01-01",
        }

        with (
            patch("trossen_cli.auth.get_token", return_value=MOCK_TOKEN),
            patch("trossen_cli.api_client.ApiClient.post", new_callable=AsyncMock) as post_mock,
            patch("trossen_cli.upload.upload_resource", new_callable=AsyncMock),
            patch("trossen_cli.upload.collect_files") as collect_mock,
        ):
            from trossen_cli.types import FileInfo

            collect_mock.return_value = [
                FileInfo(path="model.pt", size_bytes=200, content_type="application/octet-stream")
            ]
            post_mock.side_effect = [create_response, finalize_response]

            result = await create_and_upload_model(
                name="test-model",
                local_path=Path("/tmp/test"),
                privacy="public",
                base_model_id="parent-uuid",
                metadata={"arch": "act"},
                show_progress=False,
            )

        create_call = post_mock.call_args_list[0]
        payload = create_call[1]["json"]
        assert payload["parent_model_id"] == "parent-uuid"
        assert "base_model_id" not in payload
        assert payload["model_metadata"] == {"arch": "act"}
        assert result["id"] == "m-new"


class TestDownloadEndpoints:
    @pytest.mark.asyncio
    async def test_download_parses_file_download_info_array(self):
        """
        GET /{type}/{id}/download-urls returns FileDownloadInfo array.
        """
        from trossen_cli.download import download_resource

        download_response = {
            "resource_id": "ds-1",
            "files": [
                {
                    "path": "data/file.bin",
                    "size_bytes": 1024,
                    "content_type": "application/octet-stream",
                    "download_url": "https://storage.example.com/file.bin?presigned",
                    "expires_at": "2024-01-01T01:00:00Z",
                },
                {
                    "path": "metadata.json",
                    "size_bytes": 256,
                    "content_type": "application/json",
                    "download_url": "https://storage.example.com/meta.json?presigned",
                    "expires_at": "2024-01-01T01:00:00Z",
                },
            ],
            "total_size_bytes": 1280,
            "file_count": 2,
            "expires_at": "2024-01-01T01:00:00Z",
        }

        with (
            patch("trossen_cli.auth.get_token", return_value=MOCK_TOKEN),
            patch("trossen_cli.api_client.ApiClient.get", new_callable=AsyncMock) as get_mock,
            patch("trossen_cli.download.download_file", new_callable=AsyncMock) as dl_mock,
        ):
            get_mock.return_value = download_response

            await download_resource("ds-1", "datasets", Path("/tmp/out"), show_progress=False)

        get_mock.assert_called_once_with("/datasets/ds-1/download-urls")
        assert dl_mock.call_count == 2

        # Verify the download_file calls got the correct URLs
        call_urls = {call.args[1] for call in dl_mock.call_args_list}
        assert "https://storage.example.com/file.bin?presigned" in call_urls
        assert "https://storage.example.com/meta.json?presigned" in call_urls

    @pytest.mark.asyncio
    async def test_download_rejects_path_traversal(self):
        """
        Refuse to write files outside output directory.
        """
        from trossen_cli.download import DownloadError, download_resource

        traversal_response = {
            "resource_id": "ds-1",
            "files": [
                {
                    "path": "../../.bashrc",
                    "size_bytes": 100,
                    "content_type": "text/plain",
                    "download_url": "https://evil.example.com/payload",
                    "expires_at": "2024-01-01T01:00:00Z",
                },
            ],
            "total_size_bytes": 100,
            "file_count": 1,
            "expires_at": "2024-01-01T01:00:00Z",
        }

        with (
            patch("trossen_cli.auth.get_token", return_value=MOCK_TOKEN),
            patch("trossen_cli.api_client.ApiClient.get", new_callable=AsyncMock) as get_mock,
            patch("trossen_cli.download.download_file", new_callable=AsyncMock) as dl_mock,
        ):
            get_mock.return_value = traversal_response

            with pytest.raises(DownloadError, match="Path traversal detected"):
                await download_resource("ds-1", "datasets", Path("/tmp/out"), show_progress=False)

        # download_file should never have been called
        dl_mock.assert_not_called()

    @pytest.mark.asyncio
    async def test_download_rejects_absolute_path(self):
        """
        Refuse to write to absolute paths from server.
        """
        from trossen_cli.download import DownloadError, download_resource

        absolute_response = {
            "resource_id": "ds-1",
            "files": [
                {
                    "path": "/etc/cron.d/malicious",
                    "size_bytes": 50,
                    "content_type": "text/plain",
                    "download_url": "https://evil.example.com/cron",
                    "expires_at": "2024-01-01T01:00:00Z",
                },
            ],
            "total_size_bytes": 50,
            "file_count": 1,
            "expires_at": "2024-01-01T01:00:00Z",
        }

        with (
            patch("trossen_cli.auth.get_token", return_value=MOCK_TOKEN),
            patch("trossen_cli.api_client.ApiClient.get", new_callable=AsyncMock) as get_mock,
            patch("trossen_cli.download.download_file", new_callable=AsyncMock) as dl_mock,
        ):
            get_mock.return_value = absolute_response

            with pytest.raises(DownloadError, match="Absolute path not allowed"):
                await download_resource("ds-1", "datasets", Path("/tmp/out"), show_progress=False)

        dl_mock.assert_not_called()
