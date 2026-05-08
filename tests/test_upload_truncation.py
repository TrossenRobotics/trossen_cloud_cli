"""Tests for upload behavior when files are truncated/replaced mid-upload."""

import os
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from trossen_cloud_cli.types import FileInfo
from trossen_cloud_cli.upload import (
    UploadError,
    upload_part,
    upload_resource,
)


@pytest.fixture()
def small_file(tmp_path: Path) -> Path:
    """
    Write a 100-byte file used as the source for truncation tests.
    """
    p = tmp_path / "data.bin"
    p.write_bytes(b"a" * 100)
    return p


def _mock_storage_client_capturing_puts(put_calls: list):
    """
    Build a mock httpx.AsyncClient that records PUT calls and returns 200.

    Records the call up front and increments body_len per chunk so partial streams from generators
    that raise mid-iteration are still observable.
    """
    client = AsyncMock(spec=httpx.AsyncClient)

    async def mock_put(url, content=None, headers=None):
        record = {"url": url, "headers": headers, "body_len": 0}
        put_calls.append(record)
        if content is not None:
            async for chunk in content:
                record["body_len"] += len(chunk)
        response = MagicMock()
        response.headers = {"ETag": '"test-etag"'}
        response.raise_for_status = MagicMock()
        return response

    client.put = mock_put
    return client


class TestPreFlightTruncationCheck:
    """
    The pre-flight stat check should fire when file is shorter than declared.
    """

    @pytest.mark.asyncio
    async def test_pre_flight_raises_when_file_smaller_than_declared(self, small_file: Path):
        """
        upload_part is told the file is 200 bytes; on disk it's 100. The pre-flight check at the
        top of the retry loop should raise immediately and never call PUT.
        """
        put_calls: list = []
        client = _mock_storage_client_capturing_puts(put_calls)

        with pytest.raises(UploadError, match="truncated mid-upload"):
            await upload_part(
                upload_client=client,
                upload_url="https://storage.example.com/x",
                file_path=small_file,
                part_number=1,
                part_size=200,
                file_size=200,  # the lie
                progress=None,
                filename="data.bin",
            )

        assert put_calls == [], "PUT must not be issued when pre-flight fails"


class TestMidStreamEofGuard:
    """
    The streaming-body generator should raise (not silently break) on EOF.
    """

    @pytest.mark.asyncio
    async def test_mid_stream_raises_when_read_eofs_early(self, small_file: Path):
        """
        Patch Path.stat to lie about size so the pre-flight check passes, then let the real
        f.read() hit EOF before the declared chunk_size is reached. Verifies the new raise (instead
        of silent break) inside _streaming_body.
        """
        put_calls: list = []
        client = _mock_storage_client_capturing_puts(put_calls)

        # Real file is 100 bytes; lie to upload_part that it's 200.
        # We patch stat() to also report 200 so the pre-flight check passes;
        # only then can the streaming generator hit the early-EOF path.
        real_stat = os.stat_result((33188, 0, 0, 0, 0, 0, 200, 0, 0, 0))
        original_stat = Path.stat

        def fake_stat(self, *args, **kwargs):
            if self == small_file:
                return real_stat
            return original_stat(self, *args, **kwargs)

        with patch.object(Path, "stat", fake_stat):
            with pytest.raises(UploadError, match="truncated during upload"):
                await upload_part(
                    upload_client=client,
                    upload_url="https://storage.example.com/x",
                    file_path=small_file,
                    part_number=1,
                    part_size=200,
                    file_size=200,
                    progress=None,
                    filename="data.bin",
                )

        # PUT was attempted but the generator raised before completing the declared 200 bytes. The
        # 100 bytes actually on disk streamed first.
        assert len(put_calls) == 1
        assert put_calls[0]["body_len"] == 100


class TestPerFileIsolationOnTruncation:
    """
    A truncated file should fail the upload of that file but not the batch.
    """

    @pytest.mark.asyncio
    async def test_one_truncated_file_doesnt_cancel_others(self, tmp_path: Path):
        """
        Three files: one is real-but-shorter-than-declared (triggers pre-flight UploadError), two
        are real and match their declared size. Expect: UploadError summarizing 1 failed file, the
        other 2 issued PUTs.
        """
        good_a = tmp_path / "good_a.bin"
        good_b = tmp_path / "good_b.bin"
        bad = tmp_path / "bad.bin"
        good_a.write_bytes(b"a" * 100)
        good_b.write_bytes(b"b" * 100)
        bad.write_bytes(b"x" * 50)  # only 50 bytes...

        files = [
            FileInfo(path="good_a.bin", size_bytes=100, content_type="application/octet-stream"),
            FileInfo(path="good_b.bin", size_bytes=100, content_type="application/octet-stream"),
            # ...but we tell the backend it's 100, so the presigned URL expects 100.
            FileInfo(path="bad.bin", size_bytes=100, content_type="application/octet-stream"),
        ]

        async def mock_post(path, json=None):
            if "files/initiate" in path:
                return {
                    "files": [
                        {
                            "file_path": fp,
                            "total_parts": 1,
                            "part_size_bytes": 100,
                            "direct_upload_url": f"https://storage.example.com/{fp}?presigned",
                            "expires_at": "2099-01-01T00:00:00Z",
                        }
                        for fp in json["file_paths"]
                    ]
                }
            if "files/complete" in path:
                return {"files": []}
            return {}

        api_client = AsyncMock()
        api_client.post = mock_post

        put_calls: list = []
        storage_client = _mock_storage_client_capturing_puts(put_calls)

        # Patch httpx.AsyncClient so upload_resource's storage client is our mock.
        async_client_cm = MagicMock()
        async_client_cm.__aenter__ = AsyncMock(return_value=storage_client)
        async_client_cm.__aexit__ = AsyncMock(return_value=False)

        with (
            patch("trossen_cloud_cli.upload.load_upload_state", return_value=None),
            patch("trossen_cloud_cli.upload.save_upload_state"),
            patch("trossen_cloud_cli.upload.clear_upload_state"),
            patch("trossen_cloud_cli.upload.print_error"),
            patch("trossen_cloud_cli.upload.httpx.AsyncClient", return_value=async_client_cm),
        ):
            with pytest.raises(UploadError, match="1 file.*failed"):
                await upload_resource(
                    client=api_client,
                    resource_id="test-id",
                    resource_type="datasets",
                    local_path=tmp_path,
                    files=files,
                    show_progress=False,
                )

        # Both good files issued a PUT; bad.bin did not (pre-flight rejected).
        urls_put = [c["url"] for c in put_calls]
        assert any("good_a.bin" in u for u in urls_put)
        assert any("good_b.bin" in u for u in urls_put)
        assert not any("bad.bin" in u for u in urls_put)
