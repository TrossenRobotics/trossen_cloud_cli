"""Configuration management for Trossen CLI."""

import tomllib
from pathlib import Path
from typing import Any

import tomli_w
from pydantic import BaseModel, Field


class UploadConfig(BaseModel):
    """
    Upload configuration.
    """

    chunk_size_mb: int = 50
    parallel_parts: int = 6
    parallel_files: int = 32


class DownloadConfig(BaseModel):
    """
    Download configuration.
    """

    parallel_files: int = 16
    stream_chunk_size: int = 65536


class Config(BaseModel):
    """
    Main configuration model.
    """

    upload: UploadConfig = Field(default_factory=UploadConfig)
    download: DownloadConfig = Field(default_factory=DownloadConfig)


def get_config_dir() -> Path:
    """
    Get the configuration directory path.
    """
    config_dir = Path.home() / ".trossen" / "trossen_cloud_cli"
    config_dir.mkdir(parents=True, exist_ok=True)
    return config_dir


def get_config_file() -> Path:
    """
    Get the configuration file path.
    """
    return get_config_dir() / "config.toml"


def get_token_file() -> Path:
    """
    Get the token file path.
    """
    return get_config_dir() / "token"


def get_uploads_dir() -> Path:
    """
    Get the uploads state directory path.
    """
    uploads_dir = get_config_dir() / "uploads"
    uploads_dir.mkdir(parents=True, exist_ok=True)
    return uploads_dir


def load_config() -> Config:
    """
    Load configuration from file.
    """
    config_file = get_config_file()
    config_data: dict[str, Any] = {}

    # Load from file if exists
    if config_file.exists():
        with open(config_file, "rb") as f:
            config_data = tomllib.load(f)

    return Config(**config_data)


def save_config(config: Config) -> None:
    """
    Save configuration to file.
    """
    config_file = get_config_file()

    # Convert to dict for TOML serialization
    config_dict = config.model_dump()

    with open(config_file, "wb") as f:
        tomli_w.dump(config_dict, f)


def get_chunk_size_bytes(config: Config) -> int:
    """
    Get chunk size in bytes.
    """
    return config.upload.chunk_size_mb * 1024 * 1024


# Global config instance
_config: Config | None = None


def get_config() -> Config:
    """
    Get the global configuration instance.
    """
    global _config
    if _config is None:
        _config = load_config()
    return _config


def reset_config() -> None:
    """
    Reset the global configuration instance.
    """
    global _config
    _config = None
