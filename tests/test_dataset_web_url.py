"""Tests for the dataset web-URL helper used in upload output."""

import trossen_cloud_cli.commands.datasets as ds


def test_prod_default(monkeypatch):
    monkeypatch.setattr(ds, "API_BASE_URL", "https://cloud.trossen.com/api/v1")
    assert ds._dataset_web_url("abc-123") == "https://cloud.trossen.com/datasets/abc-123"


def test_dev(monkeypatch):
    monkeypatch.setattr(ds, "API_BASE_URL", "https://dev-cloud.trossen.com/api/v1")
    assert ds._dataset_web_url("id1") == "https://dev-cloud.trossen.com/datasets/id1"


def test_trailing_slash(monkeypatch):
    monkeypatch.setattr(ds, "API_BASE_URL", "https://cloud.trossen.com/api/v1/")
    assert ds._dataset_web_url("x") == "https://cloud.trossen.com/datasets/x"


def test_no_api_suffix(monkeypatch):
    # If the base URL isn't the standard /api/v1 shape, still produce a sane URL.
    monkeypatch.setattr(ds, "API_BASE_URL", "https://cloud.trossen.com")
    assert ds._dataset_web_url("x") == "https://cloud.trossen.com/datasets/x"
