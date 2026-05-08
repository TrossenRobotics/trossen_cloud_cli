"""Trossen CLI - A Python CLI for Trossen Cloud."""

from importlib.metadata import version

__version__ = version("trossen_cloud_cli")

from .cli import app

__all__ = ["app", "__version__"]
