"""Tests for output utilities."""

from trossen_cloud_cli.output import format_size


def test_format_size_bytes():
    """
    Test formatting bytes.
    """
    assert format_size(500) == "500.0 B"


def test_format_size_kilobytes():
    """
    Test formatting kilobytes.
    """
    assert format_size(1024) == "1.0 KB"
    assert format_size(1536) == "1.5 KB"


def test_format_size_megabytes():
    """
    Test formatting megabytes.
    """
    assert format_size(1024 * 1024) == "1.0 MB"
    assert format_size(52428800) == "50.0 MB"


def test_format_size_gigabytes():
    """
    Test formatting gigabytes.
    """
    assert format_size(1024 * 1024 * 1024) == "1.0 GB"


def test_format_size_terabytes():
    """
    Test formatting terabytes.
    """
    assert format_size(1024 * 1024 * 1024 * 1024) == "1.0 TB"
