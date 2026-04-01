"""Tests verifying upload behavior at scale and API payload limits.

Tests chunked batch operations, error isolation, payload size handling, and resume support.
"""

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from trossen_cli.types import FileUploadState, UploadState
from trossen_cli.upload import (
    BATCH_CHUNK_SIZE,
    UploadError,
    _upload_file_parts,
    collect_files,
    upload_resource,
)

MOCK_TOKEN = "tr_test_token_1234567890abcdef"


def _mock_file_info(count: int):
    """
    Create mock FileInfo objects.
    """
    from trossen_cli.types import FileInfo

    return [
        FileInfo(path=f"file_{i:04d}.bin", size_bytes=1024, content_type="application/octet-stream")
        for i in range(count)
    ]


def _mock_batch_initiate_response(file_paths: list[str]):
    """
    Create a mock files/initiate response with direct upload URLs.
    """
    return {
        "files": [
            {
                "file_path": fp,
                "total_parts": 1,
                "part_size_bytes": 1024,
                "direct_upload_url": f"https://storage.example.com/{fp}?presigned",
                "expires_at": "2099-01-01T00:00:00Z",
            }
            for fp in file_paths
        ]
    }


@pytest.fixture()
def _patch_upload_internals():
    """
    Patch upload internals so tests don't touch disk or storage.
    """
    with (
        patch("trossen_cli.upload.load_upload_state", return_value=None),
        patch("trossen_cli.upload.save_upload_state"),
        patch("trossen_cli.upload.clear_upload_state"),
        patch("trossen_cli.upload.upload_part", new_callable=AsyncMock, return_value='"etag"'),
        patch("trossen_cli.upload.print_error"),
    ):
        yield


class TestBatchChunking:
    """
    Verify batch operations are chunked at BATCH_CHUNK_SIZE.
    """

    @pytest.mark.asyncio
    @pytest.mark.usefixtures("_patch_upload_internals")
    async def test_batch_initiate_chunked_at_500(self):
        """
        Files exceeding BATCH_CHUNK_SIZE are split across multiple API calls.
        """
        file_count = BATCH_CHUNK_SIZE + 100  # 600 files
        files = _mock_file_info(file_count)

        post_calls = []

        async def mock_post(path, json=None):
            post_calls.append((path, json))
            if "files/initiate" in path:
                return _mock_batch_initiate_response(json["file_paths"])
            if "files/complete" in path:
                return {"files": []}
            return {}

        mock_client = AsyncMock()
        mock_client.post = mock_post

        await upload_resource(
            client=mock_client,
            resource_id="test-id",
            resource_type="datasets",
            local_path=Path("/tmp"),
            files=files,
            show_progress=False,
        )

        initiate_calls = [c for c in post_calls if "files/initiate" in c[0]]
        assert len(initiate_calls) == 2
        assert len(initiate_calls[0][1]["file_paths"]) == BATCH_CHUNK_SIZE
        assert len(initiate_calls[1][1]["file_paths"]) == 100

        complete_calls = [c for c in post_calls if "files/complete" in c[0]]
        assert len(complete_calls) == 2
        assert len(complete_calls[0][1]["file_paths"]) == BATCH_CHUNK_SIZE
        assert len(complete_calls[1][1]["file_paths"]) == 100

    @pytest.mark.asyncio
    @pytest.mark.usefixtures("_patch_upload_internals")
    async def test_exact_batch_size_single_chunk(self):
        """
        Exactly BATCH_CHUNK_SIZE files should result in 1 chunk.
        """
        files = _mock_file_info(BATCH_CHUNK_SIZE)

        post_calls = []

        async def mock_post(path, json=None):
            post_calls.append((path, json))
            if "files/initiate" in path:
                return _mock_batch_initiate_response(json["file_paths"])
            if "files/complete" in path:
                return {"files": []}
            return {}

        mock_client = AsyncMock()
        mock_client.post = mock_post

        await upload_resource(
            client=mock_client,
            resource_id="test-id",
            resource_type="datasets",
            local_path=Path("/tmp"),
            files=files,
            show_progress=False,
        )

        initiate_calls = [c for c in post_calls if "files/initiate" in c[0]]
        assert len(initiate_calls) == 1

        complete_calls = [c for c in post_calls if "files/complete" in c[0]]
        assert len(complete_calls) == 1

    @pytest.mark.asyncio
    @pytest.mark.usefixtures("_patch_upload_internals")
    async def test_prefetched_urls_skip_batch_initiate(self):
        """
        When create response includes upload_urls, files/initiate is skipped.
        """
        files = _mock_file_info(10)
        prefetched = [
            {
                "file_path": f.path,
                "total_parts": 1,
                "part_size_bytes": f.size_bytes,
                "direct_upload_url": f"https://storage.example.com/{f.path}?presigned",
                "expires_at": "2099-01-01T00:00:00Z",
            }
            for f in files
        ]

        post_calls = []

        async def mock_post(path, json=None):
            post_calls.append((path, json))
            if "files/complete" in path:
                return {"files": []}
            return {}

        mock_client = AsyncMock()
        mock_client.post = mock_post

        await upload_resource(
            client=mock_client,
            resource_id="test-id",
            resource_type="datasets",
            local_path=Path("/tmp"),
            files=files,
            show_progress=False,
            prefetched_urls=prefetched,
        )

        initiate_calls = [c for c in post_calls if "files/initiate" in c[0]]
        assert len(initiate_calls) == 0

    @pytest.mark.asyncio
    @pytest.mark.usefixtures("_patch_upload_internals")
    async def test_large_batch_3000_files(self):
        """
        3000 files should result in 6 initiate chunks and 6 complete chunks.
        """
        files = _mock_file_info(3000)

        post_calls = []

        async def mock_post(path, json=None):
            post_calls.append((path, json))
            if "files/initiate" in path:
                return _mock_batch_initiate_response(json["file_paths"])
            if "files/complete" in path:
                return {"files": []}
            return {}

        mock_client = AsyncMock()
        mock_client.post = mock_post

        await upload_resource(
            client=mock_client,
            resource_id="test-id",
            resource_type="datasets",
            local_path=Path("/tmp"),
            files=files,
            show_progress=False,
        )

        initiate_calls = [c for c in post_calls if "files/initiate" in c[0]]
        assert len(initiate_calls) == 6  # 3000 / 500 = 6

        complete_calls = [c for c in post_calls if "files/complete" in c[0]]
        assert len(complete_calls) == 6


class TestErrorIsolation:
    """
    Verify that individual file failures don't cancel the entire upload.
    """

    @pytest.mark.asyncio
    async def test_single_file_failure_doesnt_cancel_others(self):
        """
        If one file fails, others still complete and we get a count.
        """
        files = _mock_file_info(5)
        call_count = 0

        async def mock_upload_part(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 3:
                raise ConnectionError("Simulated network failure")
            return '"etag"'

        async def mock_post(path, json=None):
            if "files/initiate" in path:
                return _mock_batch_initiate_response(json["file_paths"])
            if "files/complete" in path:
                return {"files": []}
            return {}

        mock_client = AsyncMock()
        mock_client.post = mock_post

        with (
            patch("trossen_cli.upload.load_upload_state", return_value=None),
            patch("trossen_cli.upload.save_upload_state"),
            patch("trossen_cli.upload.clear_upload_state"),
            patch("trossen_cli.upload.upload_part", side_effect=mock_upload_part),
            patch("trossen_cli.upload.print_error"),
        ):
            with pytest.raises(UploadError, match="1 file.*failed"):
                await upload_resource(
                    client=mock_client,
                    resource_id="test-id",
                    resource_type="datasets",
                    local_path=Path("/tmp"),
                    files=files,
                    show_progress=False,
                )

        # All 5 files attempted, not cancelled after the 3rd
        assert call_count == 5


class TestPayloadSizeLimits:
    """
    Verify payload sizes stay under API limits.
    """

    def test_batch_chunk_size_keeps_payloads_under_10mb(self):
        """
        500 file paths at max length should be well under 10MB.
        """
        import json

        max_path = "a" * 1024
        paths = [max_path] * BATCH_CHUNK_SIZE
        payload = json.dumps({"file_paths": paths, "include_part_urls": True})
        payload_mb = len(payload) / (1024 * 1024)

        assert payload_mb < 10, f"Payload is {payload_mb:.1f} MB, exceeds 10 MB limit"

    def test_batch_chunk_size_value(self):
        """
        BATCH_CHUNK_SIZE should be 500.
        """
        assert BATCH_CHUNK_SIZE == 500


class TestCollectFilesHiddenFilter:
    """
    Verify hidden file filtering.
    """

    def test_skips_dotfiles(self, tmp_path):
        """
        collect_files should skip hidden files and directories.
        """
        (tmp_path / "visible.txt").write_text("data")
        (tmp_path / ".hidden").write_text("secret")
        (tmp_path / ".cache").mkdir()
        (tmp_path / ".cache" / "temp.bin").write_text("cached")
        (tmp_path / "subdir").mkdir()
        (tmp_path / "subdir" / ".gitkeep").write_text("")
        (tmp_path / "subdir" / "data.bin").write_text("real data")

        files = collect_files(tmp_path)
        paths = {f.path for f in files}

        assert "visible.txt" in paths
        assert "subdir/data.bin" in paths
        assert ".hidden" not in paths
        assert ".cache/temp.bin" not in paths
        assert "subdir/.gitkeep" not in paths
        assert len(files) == 2


class TestResumeUpload:
    """
    Verify that resume skips already-completed parts and avoids duplicates.
    """

    @pytest.mark.asyncio
    async def test_resume_skips_completed_parts(self, tmp_path):
        """
        Parts already recorded in state.files[fp].parts_completed
        should not be re-uploaded.
        """
        # Create a 4-part file (4 KB with 1 KB parts)
        test_file = tmp_path / "data.bin"
        test_file.write_bytes(b"\x00" * 4096)

        part_urls = {
            1: "https://storage.example.com/part1",
            2: "https://storage.example.com/part2",
            3: "https://storage.example.com/part3",
            4: "https://storage.example.com/part4",
        }

        # Simulate parts 1 and 3 already completed in a previous run
        state = UploadState(
            resource_id="res-1",
            resource_type="datasets",
            local_path=str(tmp_path),
            files={
                "data.bin": FileUploadState(
                    status="uploading",
                    parts_completed=[1, 3],
                ),
            },
        )

        uploaded_parts: list[int] = []

        async def mock_upload_part(_client, _url, _path, part_number, *args, **kwargs):
            uploaded_parts.append(part_number)
            return '"etag"'

        mock_upload = AsyncMock()

        with (
            patch("trossen_cli.upload.upload_part", side_effect=mock_upload_part),
            patch("trossen_cli.upload.save_upload_state"),
        ):
            await _upload_file_parts(
                upload_client=mock_upload,
                file_path="data.bin",
                local_path=test_file,
                part_urls=part_urls,
                part_size=1024,
                progress=None,
                state=state,
            )

        # Only parts 2 and 4 should have been uploaded
        assert sorted(uploaded_parts) == [2, 4]
        # State should now contain all 4 parts, no duplicates
        assert sorted(state.files["data.bin"].parts_completed) == [1, 2, 3, 4]

    @pytest.mark.asyncio
    async def test_resume_no_duplicate_parts_in_state(self, tmp_path):
        """
        If a part is already in parts_completed, uploading it again
        (e.g. concurrent retry) must not append a duplicate.
        """
        test_file = tmp_path / "data.bin"
        test_file.write_bytes(b"\x00" * 2048)

        # Part 1 already completed; we only provide URL for part 1
        state = UploadState(
            resource_id="res-1",
            resource_type="datasets",
            local_path=str(tmp_path),
            files={
                "data.bin": FileUploadState(
                    status="uploading",
                    parts_completed=[1],
                ),
            },
        )

        mock_upload = AsyncMock()

        with (
            patch(
                "trossen_cli.upload.upload_part",
                new_callable=AsyncMock,
                return_value='"etag"',
            ),
            patch("trossen_cli.upload.save_upload_state"),
        ):
            # Only part 2 URL provided; part 1 should be skipped
            await _upload_file_parts(
                upload_client=mock_upload,
                file_path="data.bin",
                local_path=test_file,
                part_urls={1: "https://storage.example.com/p1", 2: "https://storage.example.com/p2"},
                part_size=1024,
                progress=None,
                state=state,
            )

        # Part 1 must appear exactly once (not duplicated)
        assert state.files["data.bin"].parts_completed.count(1) == 1
        assert sorted(state.files["data.bin"].parts_completed) == [1, 2]

    @pytest.mark.asyncio
    async def test_resume_advances_progress_for_completed_parts(self, tmp_path):
        """
        On resume, progress should be advanced by the bytes already uploaded
        so the progress bar starts at the correct position.
        """
        test_file = tmp_path / "data.bin"
        test_file.write_bytes(b"\x00" * 3072)  # 3 KB, 3 parts of 1 KB

        state = UploadState(
            resource_id="res-1",
            resource_type="datasets",
            local_path=str(tmp_path),
            files={
                "data.bin": FileUploadState(
                    status="uploading",
                    parts_completed=[1, 2],  # 2 KB already done
                ),
            },
        )

        progress = MagicMock()
        mock_upload = AsyncMock()

        with (
            patch(
                "trossen_cli.upload.upload_part",
                new_callable=AsyncMock,
                return_value='"etag"',
            ),
            patch("trossen_cli.upload.save_upload_state"),
        ):
            await _upload_file_parts(
                upload_client=mock_upload,
                file_path="data.bin",
                local_path=test_file,
                part_urls={
                    1: "https://storage.example.com/p1",
                    2: "https://storage.example.com/p2",
                    3: "https://storage.example.com/p3",
                },
                part_size=1024,
                progress=progress,
                state=state,
            )

        # First call should advance by 2048 bytes (already-completed parts)
        first_call_args = progress.advance_file.call_args_list[0]
        assert first_call_args[0] == ("data.bin", 2048)

    @pytest.mark.asyncio
    async def test_resume_completed_file_skipped_entirely(self):
        """
        Files marked as 'complete' in state should be filtered out
        before upload_resource begins uploading.
        """
        files = _mock_file_info(3)

        state = UploadState(
            resource_id="test-id",
            resource_type="datasets",
            local_path="/tmp",
            files={
                files[1].path: FileUploadState(status="complete"),
            },
        )

        post_calls = []

        async def mock_post(path, json=None):
            post_calls.append((path, json))
            if "files/initiate" in path:
                return _mock_batch_initiate_response(json["file_paths"])
            if "files/complete" in path:
                return {"files": []}
            return {}

        mock_client = AsyncMock()
        mock_client.post = mock_post

        with (
            patch("trossen_cli.upload.load_upload_state", return_value=state),
            patch("trossen_cli.upload.save_upload_state"),
            patch("trossen_cli.upload.clear_upload_state"),
            patch(
                "trossen_cli.upload.upload_part",
                new_callable=AsyncMock,
                return_value='"etag"',
            ),
        ):
            await upload_resource(
                client=mock_client,
                resource_id="test-id",
                resource_type="datasets",
                local_path=Path("/tmp"),
                files=files,
                show_progress=False,
            )

        # Only 2 files should have been initiated (the completed one is skipped)
        initiate_calls = [c for c in post_calls if "files/initiate" in c[0]]
        assert len(initiate_calls) == 1
        initiated_paths = initiate_calls[0][1]["file_paths"]
        assert len(initiated_paths) == 2
        assert files[1].path not in initiated_paths
