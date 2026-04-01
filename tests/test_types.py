"""Tests for types module."""

from trossen_cli.types import (
    DatasetInfo,
    DatasetType,
    FileInfo,
    FileUploadState,
    ModelInfo,
    PrivacyLevel,
    UploadState,
    UploadStatus,
)


def test_dataset_type_values():
    """
    Test DatasetType enum values.
    """
    assert DatasetType.MCAP == "mcap"
    assert DatasetType.LEROBOT == "lerobot"
    assert DatasetType.TROSSEN == "trossen"
    assert DatasetType.RAW == "raw"


def test_privacy_level_values():
    """
    Test PrivacyLevel enum values.
    """
    assert PrivacyLevel.PRIVATE == "private"
    assert PrivacyLevel.PUBLIC == "public"


def test_upload_status_values():
    """
    Test UploadStatus enum values.
    """
    assert UploadStatus.PENDING == "pending"
    assert UploadStatus.UPLOADING == "uploading"
    assert UploadStatus.PROCESSING == "processing"
    assert UploadStatus.COMPLETE == "complete"
    assert UploadStatus.FAILED == "failed"
    assert UploadStatus.CANCELLED == "cancelled"


def test_file_info():
    """
    Test FileInfo model.
    """
    file_info = FileInfo(
        path="data/test.parquet",
        size_bytes=1024,
        content_type="application/octet-stream",
    )
    assert file_info.path == "data/test.parquet"
    assert file_info.size_bytes == 1024
    assert file_info.content_type == "application/octet-stream"


def test_file_info_default_content_type():
    """
    Test FileInfo default content type.
    """
    file_info = FileInfo(path="test.bin", size_bytes=100)
    assert file_info.content_type == "application/octet-stream"


def test_upload_state():
    """
    Test UploadState model.
    """
    state = UploadState(
        resource_id="abc123",
        resource_type="datasets",
        local_path="/path/to/data",
    )
    assert state.resource_id == "abc123"
    assert state.resource_type == "datasets"
    assert state.local_path == "/path/to/data"
    assert state.files == {}


def test_upload_state_with_files():
    """
    Test UploadState with files.
    """
    state = UploadState(
        resource_id="abc123",
        resource_type="datasets",
        local_path="/path/to/data",
        files={
            "file1.bin": FileUploadState(status="complete", parts_completed=[1, 2, 3]),
            "file2.bin": FileUploadState(status="uploading", parts_completed=[1]),
        },
    )
    assert len(state.files) == 2
    assert state.files["file1.bin"].status == "complete"
    assert state.files["file2.bin"].parts_completed == [1]


def test_dataset_info():
    """
    Test DatasetInfo model.
    """
    info = DatasetInfo(
        id="dataset-123",
        name="Test Dataset",
        type=DatasetType.LEROBOT,
        privacy=PrivacyLevel.PRIVATE,
        user_id="user-456",
        created_at="2024-01-01T00:00:00Z",
    )
    assert info.id == "dataset-123"
    assert info.name == "Test Dataset"
    assert info.type == DatasetType.LEROBOT
    assert info.privacy == PrivacyLevel.PRIVATE
    assert info.user_id == "user-456"


def test_model_info():
    """
    Test ModelInfo model.
    """
    info = ModelInfo(
        id="model-123",
        name="Test Model",
        privacy=PrivacyLevel.PUBLIC,
        user_id="user-456",
        created_at="2024-01-01T00:00:00Z",
        parent_model_id="base-789",
    )
    assert info.id == "model-123"
    assert info.name == "Test Model"
    assert info.parent_model_id == "base-789"
