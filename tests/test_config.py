"""Tests for configuration management."""

import importlib

from typer.testing import CliRunner

import trossen_cloud_cli.api_client
from trossen_cloud_cli.cli import app
from trossen_cloud_cli.config import (
    Config,
    DownloadConfig,
    UploadConfig,
    get_chunk_size_bytes,
    load_config,
    save_config,
)

runner = CliRunner()


def test_default_config():
    """
    Test default configuration values.
    """
    config = Config()

    assert config.upload.chunk_size_mb == 50
    assert config.upload.parallel_parts == 6
    assert config.upload.parallel_files == 32
    assert config.download.parallel_files == 16


def test_chunk_size_bytes():
    """
    Test chunk size conversion to bytes.
    """
    config = Config()
    assert get_chunk_size_bytes(config) == 50 * 1024 * 1024


def test_env_var_override(monkeypatch):
    """
    Test API URL environment variable override.
    """
    # Ensure TROSSEN_API_URL is not set, then reload api_client module to make test deterministic
    # regardless of environment
    monkeypatch.delenv("TROSSEN_API_URL", raising=False)
    importlib.reload(trossen_cloud_cli.api_client)

    from trossen_cloud_cli.api_client import API_BASE_URL

    # Default value when env var not set during import
    assert "cloud.trossen.com" in API_BASE_URL


def test_upload_config():
    """
    Test upload configuration.
    """
    config = UploadConfig(chunk_size_mb=100, parallel_parts=8, parallel_files=4)
    assert config.chunk_size_mb == 100
    assert config.parallel_parts == 8
    assert config.parallel_files == 4


def test_download_config():
    """
    Test download configuration.
    """
    config = DownloadConfig(parallel_files=8)
    assert config.parallel_files == 8


class TestConfigShow:
    def test_show_displays_values(self):
        result = runner.invoke(app, ["config", "show"])
        assert result.exit_code == 0
        assert "chunk_size_mb" in result.stdout
        assert "parallel_parts" in result.stdout
        assert "parallel_files" in result.stdout
        assert "stream_chunk_size" in result.stdout

    def test_show_displays_defaults(self):
        result = runner.invoke(app, ["config", "show"])
        assert "50" in result.stdout  # default chunk_size_mb
        assert "6" in result.stdout  # default parallel_parts


class TestConfigSet:
    def test_set_valid_key(self, tmp_path, monkeypatch):
        config_file = tmp_path / "config.toml"
        monkeypatch.setattr("trossen_cloud_cli.config.get_config_file", lambda: config_file)
        monkeypatch.setattr("trossen_cloud_cli.commands.config.load_config", load_config)
        monkeypatch.setattr("trossen_cloud_cli.commands.config.save_config", save_config)

        result = runner.invoke(app, ["config", "set", "upload.chunk_size_mb", "100"])
        assert result.exit_code == 0
        assert "100" in result.stdout

        # Verify it persisted
        config = load_config()
        assert config.upload.chunk_size_mb == 100

    def test_set_unknown_key(self):
        result = runner.invoke(app, ["config", "set", "bad.key", "100"])
        assert result.exit_code == 1
        assert "Unknown key" in result.stdout

    def test_set_invalid_value(self):
        result = runner.invoke(app, ["config", "set", "upload.chunk_size_mb", "abc"])
        assert result.exit_code == 1
        assert "Invalid value" in result.stdout

    def test_set_zero_rejected(self, tmp_path, monkeypatch):
        config_file = tmp_path / "config.toml"
        monkeypatch.setattr("trossen_cloud_cli.config.get_config_file", lambda: config_file)

        result = runner.invoke(app, ["config", "set", "upload.chunk_size_mb", "0"])
        assert result.exit_code == 1
        assert "positive" in result.stdout

    def test_set_negative_rejected(self, tmp_path, monkeypatch):
        config_file = tmp_path / "config.toml"
        monkeypatch.setattr("trossen_cloud_cli.config.get_config_file", lambda: config_file)

        result = runner.invoke(app, ["config", "set", "upload.parallel_parts", "--", "-1"])
        assert result.exit_code == 1
        assert "positive" in result.stdout


class TestConfigReset:
    def test_reset_with_force(self, tmp_path, monkeypatch):
        config_file = tmp_path / "config.toml"
        monkeypatch.setattr("trossen_cloud_cli.config.get_config_file", lambda: config_file)
        monkeypatch.setattr("trossen_cloud_cli.commands.config.save_config", save_config)

        # First set a non-default value
        save_config(Config(upload=UploadConfig(chunk_size_mb=200)))

        # Reset
        result = runner.invoke(app, ["config", "reset", "--force"])
        assert result.exit_code == 0
        assert "reset" in result.stdout.lower()

        # Verify defaults restored
        config = load_config()
        assert config.upload.chunk_size_mb == 50

    def test_reset_prompts_without_force(self):
        result = runner.invoke(app, ["config", "reset"], input="n\n")
        assert result.exit_code == 1  # Aborted
