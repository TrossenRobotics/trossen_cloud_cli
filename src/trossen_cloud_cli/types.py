"""Shared types and enums for Trossen CLI."""

from enum import StrEnum

from pydantic import BaseModel, Field


class DatasetType(StrEnum):
    """
    Supported dataset types.
    """

    MCAP = "mcap"
    LEROBOT = "lerobot"


class PrivacyLevel(StrEnum):
    """
    Privacy levels for datasets and models.
    """

    PRIVATE = "private"
    PUBLIC = "public"


class UploadStatus(StrEnum):
    """
    Status of an upload operation.
    """

    PENDING = "pending"
    UPLOADING = "uploading"
    PROCESSING = "processing"
    COMPLETE = "complete"
    FAILED = "failed"
    CANCELLED = "cancelled"


class FileInfo(BaseModel):
    """
    Information about a file to upload.
    """

    path: str
    size_bytes: int
    content_type: str = "application/octet-stream"


class FileUploadState(BaseModel):
    """
    State of a single file upload for resume support.
    """

    status: str = "pending"
    parts_completed: list[int] = Field(default_factory=list)


class UploadState(BaseModel):
    """
    State of an upload operation for resume support.
    """

    resource_id: str
    resource_type: str
    local_path: str
    files: dict[str, FileUploadState] = Field(default_factory=dict)


class TrainingJobStatus(StrEnum):
    """
    Status of a training job.
    """

    PENDING = "pending"
    QUEUED = "queued"
    INITIALIZED = "initialized"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELED = "canceled"


class TrainingJobRunnerType(StrEnum):
    """
    Training job runner platforms.
    """

    SAGEMAKER = "sagemaker"


class DatasetInfo(BaseModel):
    """
    Dataset information returned from API.
    """

    id: str
    name: str
    type: DatasetType
    privacy: PrivacyLevel
    user_id: str | None = None
    dataset_metadata: dict[str, str] = Field(default_factory=dict)
    created_at: str | None = None
    updated_at: str | None = None


class ModelInfo(BaseModel):
    """
    Model information returned from API.
    """

    id: str
    name: str
    privacy: PrivacyLevel
    model_metadata: dict[str, str] = Field(default_factory=dict)
    parent_model_id: str | None = None
    user_id: str | None = None
    created_at: str | None = None
    updated_at: str | None = None


class InitiateUploadResponse(BaseModel):
    """
    Response from initiating a file upload.
    """

    total_parts: int
    part_size_bytes: int


class PartUploadResponse(BaseModel):
    """
    Response from getting a part upload URL.
    """

    upload_url: str
    part_number: int
    expires_at: str | None = None


class FileDownloadInfo(BaseModel):
    """
    Information about a single downloadable file.
    """

    path: str
    size_bytes: int
    content_type: str
    download_url: str
    expires_at: str


class StoredToken(BaseModel):
    """
    Stored API token.
    """

    token: str
