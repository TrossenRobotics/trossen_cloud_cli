"""Tests for `trc dataset episodes` and `trc dataset remove-episodes`.

Uses unittest.mock to patch ApiClient.get/.post and auth so command flow can be
verified without real network access.
"""

from unittest.mock import AsyncMock, patch

from typer.testing import CliRunner

from trossen_cloud_cli.cli import app

runner = CliRunner()

MOCK_TOKEN = "tr_test_token_1234567890abcdef"
DATASET_ID = "11111111-1111-1111-1111-111111111111"


def _episode(idx: int, ep_id: str | None = None) -> dict:
    return {
        "id": ep_id or f"ep-{idx:06d}",
        "dataset_id": DATASET_ID,
        "source_key": f"episode_{idx:06d}.mcap",
        "source_size_bytes": 1000 + idx,
        "duration_seconds": None,
        "viz": None,
    }


def _resolve_ok():
    """Patch resolve to return a fixed UUID (bypass the /datasets GET)."""
    return patch(
        "trossen_cloud_cli.commands.datasets.resolve_dataset_identifier",
        new=AsyncMock(return_value={"id": DATASET_ID}),
    )


def _list_returns(items, total=None):
    """Patch _fetch_all_episodes via ApiClient.get returning one page.

    Patches the class reference the command module actually uses; another test
    (test_config) reloads the api_client module, so patching
    ``trossen_cloud_cli.api_client.ApiClient`` directly can miss the class the
    command holds.
    """
    page = {"items": items, "total": total if total is not None else len(items)}
    return patch(
        "trossen_cloud_cli.commands.datasets.ApiClient.get", new=AsyncMock(return_value=page)
    )


# -- episodes (list) --


def test_episodes_list_render():
    items = [_episode(0), _episode(1), _episode(2)]
    with (
        patch("trossen_cloud_cli.auth.get_token", return_value=MOCK_TOKEN),
        _resolve_ok(),
        _list_returns(items),
    ):
        result = runner.invoke(app, ["dataset", "episodes", DATASET_ID])
    assert result.exit_code == 0, result.stdout
    assert "Episodes (3)" in result.stdout
    assert "episode_000000.mcap" in result.stdout
    assert "episode_000002.mcap" in result.stdout


def test_episodes_empty():
    with (
        patch("trossen_cloud_cli.auth.get_token", return_value=MOCK_TOKEN),
        _resolve_ok(),
        _list_returns([]),
    ):
        result = runner.invoke(app, ["dataset", "episodes", DATASET_ID])
    assert result.exit_code == 0
    assert "No episodes found" in result.stdout


def test_episodes_pagination():
    """_fetch_all_episodes must page past the 200-item cap."""
    page1 = {"items": [_episode(i) for i in range(200)], "total": 250}
    page2 = {"items": [_episode(i) for i in range(200, 250)], "total": 250}
    get_mock = AsyncMock(side_effect=[page1, page2])
    with (
        patch("trossen_cloud_cli.auth.get_token", return_value=MOCK_TOKEN),
        _resolve_ok(),
        patch("trossen_cloud_cli.commands.datasets.ApiClient.get", new=get_mock),
    ):
        result = runner.invoke(app, ["dataset", "episodes", DATASET_ID])
    assert result.exit_code == 0
    assert "Episodes (250)" in result.stdout
    assert get_mock.await_count == 2
    # second call used offset=200
    assert get_mock.await_args_list[1].kwargs["params"]["offset"] == 200


def test_episodes_pagination_without_total():
    """A full page with no `total` field must not stop paging early."""
    page1 = {"items": [_episode(i) for i in range(200)]}  # no "total"
    page2 = {"items": [_episode(i) for i in range(200, 230)]}  # short page → stop
    get_mock = AsyncMock(side_effect=[page1, page2])
    with (
        patch("trossen_cloud_cli.auth.get_token", return_value=MOCK_TOKEN),
        _resolve_ok(),
        patch("trossen_cloud_cli.commands.datasets.ApiClient.get", new=get_mock),
    ):
        result = runner.invoke(app, ["dataset", "episodes", DATASET_ID])
    assert result.exit_code == 0
    assert "Episodes (230)" in result.stdout
    assert get_mock.await_count == 2


# -- remove-episodes --


def _run_remove(args, list_items, post_return=None, input=None):
    """Helper: invoke remove-episodes with mocked list GET + remove POST."""
    page = {"items": list_items, "total": len(list_items)}
    get_mock = AsyncMock(return_value=page)
    post_mock = AsyncMock(
        return_value=post_return
        or {"removed": [], "not_found": [], "file_count": len(list_items), "total_size_bytes": 0}
    )
    with (
        patch("trossen_cloud_cli.auth.get_token", return_value=MOCK_TOKEN),
        _resolve_ok(),
        patch("trossen_cloud_cli.commands.datasets.ApiClient.get", new=get_mock),
        patch("trossen_cloud_cli.commands.datasets.ApiClient.post", new=post_mock),
    ):
        result = runner.invoke(app, ["dataset", "remove-episodes", *args], input=input)
    return result, post_mock


def test_remove_name_resolution():
    items = [_episode(42, "ep-42")]
    result, post = _run_remove(
        [DATASET_ID, "episode_000042.mcap", "--force"],
        items,
        post_return={"removed": ["ep-42"], "not_found": [], "file_count": 0, "total_size_bytes": 0},
    )
    assert result.exit_code == 0, result.stdout
    assert post.await_args.kwargs["json"] == {"episode_ids": ["ep-42"]}
    assert "Removed 1 episode(s)" in result.stdout


def test_remove_bare_form_resolves():
    items = [_episode(42, "ep-42")]
    result, post = _run_remove(
        [DATASET_ID, "episode_000042", "--force"],
        items,
        post_return={"removed": ["ep-42"], "not_found": [], "file_count": 0, "total_size_bytes": 0},
    )
    assert result.exit_code == 0
    assert post.await_args.kwargs["json"] == {"episode_ids": ["ep-42"]}


def test_remove_windows_path_and_case_resolves():
    """A Windows-style path and uppercase .MCAP still resolve to the source_key."""
    items = [_episode(42, "ep-42")]
    result, post = _run_remove(
        [DATASET_ID, "subdir\\episode_000042.MCAP", "--force"],
        items,
        post_return={"removed": ["ep-42"], "not_found": [], "file_count": 0, "total_size_bytes": 0},
    )
    assert result.exit_code == 0, result.stdout
    assert post.await_args.kwargs["json"] == {"episode_ids": ["ep-42"]}


def test_remove_duplicate_input_deduped():
    items = [_episode(42, "ep-42")]
    result, post = _run_remove(
        [DATASET_ID, "episode_000042.mcap", "episode_000042", "--force"],
        items,
        post_return={"removed": ["ep-42"], "not_found": [], "file_count": 0, "total_size_bytes": 0},
    )
    assert result.exit_code == 0
    assert post.await_args.kwargs["json"] == {"episode_ids": ["ep-42"]}


def test_remove_unresolved_warns_but_continues():
    items = [_episode(42, "ep-42")]
    result, post = _run_remove(
        [DATASET_ID, "episode_000042.mcap", "episode_999999.mcap", "--force"],
        items,
        post_return={"removed": ["ep-42"], "not_found": [], "file_count": 0, "total_size_bytes": 0},
    )
    assert result.exit_code == 0
    assert "No episode matching 'episode_999999.mcap'" in result.stdout
    assert post.await_args.kwargs["json"] == {"episode_ids": ["ep-42"]}


def test_remove_no_match_errors_no_post():
    items = [_episode(42, "ep-42")]
    result, post = _run_remove([DATASET_ID, "episode_999999.mcap", "--force"], items)
    assert result.exit_code == 1
    assert "No matching episodes found" in result.stdout
    post.assert_not_called()


def test_remove_confirm_declined_no_post():
    items = [_episode(42, "ep-42")]
    result, post = _run_remove([DATASET_ID, "episode_000042.mcap"], items, input="n\n")
    assert result.exit_code == 0
    post.assert_not_called()


def test_remove_confirm_accepted():
    items = [_episode(42, "ep-42")]
    result, post = _run_remove(
        [DATASET_ID, "episode_000042.mcap"],
        items,
        post_return={"removed": ["ep-42"], "not_found": [], "file_count": 0, "total_size_bytes": 0},
        input="y\n",
    )
    assert result.exit_code == 0
    post.assert_awaited_once()


def test_remove_over_200_rejected():
    items = [_episode(i, f"ep-{i}") for i in range(201)]
    names = [f"episode_{i:06d}.mcap" for i in range(201)]
    result, post = _run_remove([DATASET_ID, *names, "--force"], items)
    assert result.exit_code == 1
    assert "exceeds the 200-per-call limit" in result.stdout
    post.assert_not_called()


def test_remove_response_rendering():
    items = [_episode(42, "ep-42")]
    result, _ = _run_remove(
        [DATASET_ID, "episode_000042.mcap", "--force"],
        items,
        post_return={
            "removed": ["ep-42"],
            "not_found": ["ep-gone"],
            "file_count": 7,
            "total_size_bytes": 12345,
        },
    )
    assert result.exit_code == 0
    assert "Removed 1 episode(s)" in result.stdout
    assert "1 episode(s) were already gone" in result.stdout
    assert "Remaining:" in result.stdout
    assert "7 files" in result.stdout


def test_remove_pagination_resolves_page2():
    """A name only on page 2 must still resolve (guards the 200-cap bug)."""
    page1 = {"items": [_episode(i, f"ep-{i}") for i in range(200)], "total": 201}
    page2 = {"items": [_episode(200, "ep-200")], "total": 201}
    get_mock = AsyncMock(side_effect=[page1, page2])
    post_mock = AsyncMock(
        return_value={
            "removed": ["ep-200"],
            "not_found": [],
            "file_count": 200,
            "total_size_bytes": 0,
        }
    )
    with (
        patch("trossen_cloud_cli.auth.get_token", return_value=MOCK_TOKEN),
        _resolve_ok(),
        patch("trossen_cloud_cli.commands.datasets.ApiClient.get", new=get_mock),
        patch("trossen_cloud_cli.commands.datasets.ApiClient.post", new=post_mock),
    ):
        result = runner.invoke(
            app, ["dataset", "remove-episodes", DATASET_ID, "episode_000200.mcap", "--force"]
        )
    assert result.exit_code == 0, result.stdout
    assert get_mock.await_count == 2
    assert post_mock.await_args.kwargs["json"] == {"episode_ids": ["ep-200"]}


def test_command_help():
    result = runner.invoke(app, ["dataset", "remove-episodes", "--help"])
    assert result.exit_code == 0
    assert "episode" in result.stdout.lower()
