"""Tests for CLI commands."""

from typer.testing import CliRunner

from trossen_cli.cli import app

runner = CliRunner()


def test_app_help():
    """
    Test main app help.
    """
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    assert "Trossen Cloud" in result.stdout


def test_auth_help():
    """
    Test auth subcommand help.
    """
    result = runner.invoke(app, ["auth", "--help"])
    assert result.exit_code == 0
    assert "login" in result.stdout
    assert "logout" in result.stdout
    assert "status" in result.stdout


def test_config_help():
    """
    Test config subcommand help.
    """
    result = runner.invoke(app, ["config", "--help"])
    assert result.exit_code == 0
    assert "show" in result.stdout
    assert "set" in result.stdout
    assert "reset" in result.stdout


def test_dataset_help():
    """
    Test dataset subcommand help.
    """
    result = runner.invoke(app, ["dataset", "--help"])
    assert result.exit_code == 0
    assert "upload" in result.stdout
    assert "download" in result.stdout
    assert "list" in result.stdout
    assert "view" in result.stdout
    assert "info" in result.stdout
    assert "update" in result.stdout
    assert "delete" in result.stdout


def test_model_help():
    """
    Test model subcommand help.
    """
    result = runner.invoke(app, ["model", "--help"])
    assert result.exit_code == 0
    assert "upload" in result.stdout
    assert "download" in result.stdout
    assert "list" in result.stdout
    assert "view" in result.stdout
    assert "info" in result.stdout
    assert "update" in result.stdout
    assert "delete" in result.stdout


def test_training_job_help():
    """
    Test training-job subcommand help.
    """
    result = runner.invoke(app, ["training-job", "--help"])
    assert result.exit_code == 0
    assert "create" in result.stdout
    assert "list" in result.stdout
    assert "info" in result.stdout
    assert "cancel" in result.stdout
    assert "models" in result.stdout


def test_login_help():
    """
    Test login command help.
    """
    result = runner.invoke(app, ["auth", "login", "--help"])
    assert result.exit_code == 0
    assert "token" in result.stdout.lower()


def test_logout_help():
    """
    Test logout command help.
    """
    result = runner.invoke(app, ["auth", "logout", "--help"])
    assert result.exit_code == 0


def test_status_help():
    """
    Test status command help.
    """
    result = runner.invoke(app, ["auth", "status", "--help"])
    assert result.exit_code == 0
