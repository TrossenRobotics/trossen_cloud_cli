"""Tests for dataset validators."""

import json
import struct
from pathlib import Path
from unittest.mock import patch

from typer.testing import CliRunner

from trossen_cloud_cli.cli import app
from trossen_cloud_cli.types import DatasetType
from trossen_cloud_cli.validators import validate_dataset
from trossen_cloud_cli.validators.lerobot import validate_lerobot
from trossen_cloud_cli.validators.mcap import MCAP_MAGIC, validate_mcap

runner = CliRunner()

# ── Helpers ──────────────────────────────────────────────────────────────────


def _make_info_json(overrides: dict | None = None) -> dict:
    """Create a valid LeRobot v3 info.json."""
    info = {
        "codebase_version": "v3.0",
        "robot_type": "so100",
        "total_episodes": 2,
        "total_frames": 100,
        "total_tasks": 1,
        "chunks_size": 1000,
        "data_path": "data/chunk-{chunk_index:03d}/file-{file_index:03d}.parquet",
        "video_path": "videos/{video_key}/chunk-{chunk_index:03d}/file-{file_index:03d}.mp4",
        "data_files_size_in_mb": 100,
        "video_files_size_in_mb": 500,
        "fps": 30,
        "splits": {"train": "0:2"},
        "features": {
            "timestamp": {"dtype": "float32", "shape": [1], "names": None},
            "frame_index": {"dtype": "int64", "shape": [1], "names": None},
            "episode_index": {"dtype": "int64", "shape": [1], "names": None},
            "index": {"dtype": "int64", "shape": [1], "names": None},
            "task_index": {"dtype": "int64", "shape": [1], "names": None},
            "action": {"dtype": "float32", "shape": [6], "names": None},
            "observation.state": {"dtype": "float32", "shape": [6], "names": None},
        },
    }
    if overrides:
        info.update(overrides)
    return info


def _make_valid_lerobot(tmp_path: Path, info_overrides: dict | None = None) -> Path:
    """Create a minimal valid LeRobot v3 dataset directory."""
    ds = tmp_path / "dataset"
    ds.mkdir()

    # meta/info.json
    meta = ds / "meta"
    meta.mkdir()
    info = _make_info_json(info_overrides)
    (meta / "info.json").write_text(json.dumps(info))

    # meta/stats.json
    full_stat = {
        "min": [0],
        "max": [1],
        "mean": [0.5],
        "std": [0.1],
        "count": [100],
        "q01": [0.01],
        "q10": [0.1],
        "q50": [0.5],
        "q90": [0.9],
        "q99": [0.99],
    }
    stats = {
        "action": full_stat,
        "observation.state": full_stat,
    }
    (meta / "stats.json").write_text(json.dumps(stats))

    # meta/tasks.parquet (just a placeholder file)
    (meta / "tasks.parquet").write_bytes(b"PAR1placeholder")

    # meta/episodes/
    episodes = meta / "episodes" / "chunk-000"
    episodes.mkdir(parents=True)
    (episodes / "episode_000000.parquet").write_bytes(b"PAR1placeholder")

    # data/
    data = ds / "data" / "chunk-000"
    data.mkdir(parents=True)
    (data / "episode_000000.parquet").write_bytes(b"PAR1placeholder")

    return ds


def _write_mcap_header(profile: str = "trossen", library: str = "test") -> bytes:
    """Build MCAP magic + header record bytes."""
    # Build header content: profile string + library string (uint32 LE prefix each)
    profile_bytes = profile.encode("utf-8")
    library_bytes = library.encode("utf-8")
    content = (
        struct.pack("<I", len(profile_bytes))
        + profile_bytes
        + struct.pack("<I", len(library_bytes))
        + library_bytes
    )
    # Header record: opcode 0x01 + uint64 content length + content
    record = bytes([0x01]) + struct.pack("<Q", len(content)) + content
    return record


def _write_mcap_footer() -> bytes:
    """Build a minimal MCAP footer record."""
    # Footer opcode 0x02, content: summary_start(u64) + summary_offset_start(u64) + summary_crc(u32)
    content = struct.pack("<QQI", 0, 0, 0)
    return bytes([0x02]) + struct.pack("<Q", len(content)) + content


def _write_mcap_data_end() -> bytes:
    """Build a DataEnd record."""
    # DataEnd opcode 0x0F, content: data_section_crc (u32)
    content = struct.pack("<I", 0)
    return bytes([0x0F]) + struct.pack("<Q", len(content)) + content


def _make_valid_mcap_file(
    path: Path,
    profile: str = "trossen",
) -> None:
    """Create a minimal valid Trossen MCAP file."""
    data = MCAP_MAGIC
    data += _write_mcap_header(profile)
    data += _write_mcap_data_end()
    data += _write_mcap_footer()
    data += MCAP_MAGIC
    path.write_bytes(data)


def _make_valid_mcap_dataset(tmp_path: Path, num_episodes: int = 3) -> Path:
    """Create a minimal valid Trossen MCAP dataset directory."""
    ds = tmp_path / "dataset"
    ds.mkdir()
    for i in range(num_episodes):
        _make_valid_mcap_file(ds / f"episode_{i:06d}.mcap")
    return ds


# ── Dispatcher tests ─────────────────────────────────────────────────────────


class TestValidateDataset:
    def test_dispatches_to_lerobot(self, tmp_path):
        ds = _make_valid_lerobot(tmp_path)
        warnings = validate_dataset(ds, DatasetType.LEROBOT)
        assert warnings == []

    def test_dispatches_to_mcap(self, tmp_path):
        ds = _make_valid_mcap_dataset(tmp_path)
        warnings = validate_dataset(ds, DatasetType.MCAP)
        assert warnings == []


# ── LeRobot v3 validator tests ───────────────────────────────────────────────


class TestLeRobotValidator:
    def test_valid_dataset_no_warnings(self, tmp_path):
        ds = _make_valid_lerobot(tmp_path)
        assert validate_lerobot(ds) == []

    def test_not_a_directory(self, tmp_path):
        f = tmp_path / "file.txt"
        f.write_text("hello")
        warnings = validate_lerobot(f)
        assert any("Expected a directory" in w for w in warnings)

    def test_missing_info_json(self, tmp_path):
        ds = tmp_path / "dataset"
        ds.mkdir()
        (ds / "meta").mkdir()
        warnings = validate_lerobot(ds)
        assert any("meta/info.json" in w for w in warnings)

    def test_invalid_json_in_info(self, tmp_path):
        ds = tmp_path / "dataset"
        ds.mkdir()
        meta = ds / "meta"
        meta.mkdir()
        (meta / "info.json").write_text("{invalid json")
        warnings = validate_lerobot(ds)
        assert any("Invalid JSON" in w for w in warnings)

    def test_missing_required_fields(self, tmp_path):
        ds = tmp_path / "dataset"
        ds.mkdir()
        meta = ds / "meta"
        meta.mkdir()
        (meta / "info.json").write_text(json.dumps({"codebase_version": "v3.0"}))
        warnings = validate_lerobot(ds)
        assert any("missing required fields" in w for w in warnings)

    def test_wrong_codebase_version(self, tmp_path):
        ds = _make_valid_lerobot(tmp_path, {"codebase_version": "v2.1"})
        warnings = validate_lerobot(ds)
        assert any("v2.1" in w and "expected v3" in w for w in warnings)

    def test_v3_subversions_accepted(self, tmp_path):
        ds = _make_valid_lerobot(tmp_path, {"codebase_version": "v3.1"})
        warnings = validate_lerobot(ds)
        assert not any("codebase_version" in w for w in warnings)

    def test_zero_fps_warning(self, tmp_path):
        ds = _make_valid_lerobot(tmp_path, {"fps": 0})
        warnings = validate_lerobot(ds)
        assert any("fps is 0" in w for w in warnings)

    def test_negative_total_frames(self, tmp_path):
        ds = _make_valid_lerobot(tmp_path, {"total_frames": -1})
        warnings = validate_lerobot(ds)
        assert any("total_frames" in w and "non-negative" in w for w in warnings)

    def test_empty_features_dict(self, tmp_path):
        ds = _make_valid_lerobot(tmp_path, {"features": {}})
        warnings = validate_lerobot(ds)
        assert any("features dict is empty" in w for w in warnings)

    def test_feature_missing_dtype(self, tmp_path):
        features = _make_info_json()["features"]
        features["bad_feat"] = {"shape": [3]}
        ds = _make_valid_lerobot(tmp_path, {"features": features})
        warnings = validate_lerobot(ds)
        assert any("bad_feat" in w and "dtype" in w for w in warnings)

    def test_feature_missing_shape(self, tmp_path):
        features = _make_info_json()["features"]
        features["bad_feat"] = {"dtype": "float32"}
        ds = _make_valid_lerobot(tmp_path, {"features": features})
        warnings = validate_lerobot(ds)
        assert any("bad_feat" in w and "shape" in w for w in warnings)

    def test_feature_unrecognized_dtype(self, tmp_path):
        features = _make_info_json()["features"]
        features["bad_feat"] = {"dtype": "complex128", "shape": [1]}
        ds = _make_valid_lerobot(tmp_path, {"features": features})
        warnings = validate_lerobot(ds)
        assert any("unrecognized dtype" in w and "complex128" in w for w in warnings)

    def test_feature_shape_not_positive_ints(self, tmp_path):
        features = _make_info_json()["features"]
        features["bad_feat"] = {"dtype": "float32", "shape": [0, -1]}
        ds = _make_valid_lerobot(tmp_path, {"features": features})
        warnings = validate_lerobot(ds)
        assert any("positive integers" in w for w in warnings)

    def test_missing_action_feature(self, tmp_path):
        features = _make_info_json()["features"]
        del features["action"]
        ds = _make_valid_lerobot(tmp_path, {"features": features})
        warnings = validate_lerobot(ds)
        assert any("'action' feature" in w for w in warnings)

    def test_feature_name_with_slash(self, tmp_path):
        features = _make_info_json()["features"]
        features["bad/name"] = {"dtype": "float32", "shape": [1]}
        ds = _make_valid_lerobot(tmp_path, {"features": features})
        warnings = validate_lerobot(ds)
        assert any("bad/name" in w and "/" in w for w in warnings)

    def test_video_feature_missing_video_path(self, tmp_path):
        features = _make_info_json()["features"]
        features["observation.images.top"] = {
            "dtype": "video",
            "shape": [480, 640, 3],
            "info": {},
        }
        ds = _make_valid_lerobot(tmp_path, {"features": features})
        # Remove video_path from the already-written info.json
        info = json.loads((ds / "meta" / "info.json").read_text())
        del info["video_path"]
        (ds / "meta" / "info.json").write_text(json.dumps(info))
        (ds / "videos" / "observation.images.top" / "chunk-000").mkdir(parents=True)
        (ds / "videos" / "observation.images.top" / "chunk-000" / "file-000.mp4").write_bytes(
            b"fake"
        )
        warnings = validate_lerobot(ds)
        assert any("video_path" in w for w in warnings)

    def test_video_feature_with_video_info_key(self, tmp_path):
        features = _make_info_json()["features"]
        features["observation.images.top"] = {
            "dtype": "video",
            "shape": [480, 640, 3],
            "video_info": {"video.fps": 30.0, "video.codec": "av1"},
        }
        ds = _make_valid_lerobot(tmp_path, {"features": features})
        (ds / "videos" / "observation.images.top" / "chunk-000").mkdir(parents=True)
        (ds / "videos" / "observation.images.top" / "chunk-000" / "file-000.mp4").write_bytes(
            b"fake"
        )
        warnings = validate_lerobot(ds)
        assert not any("missing 'info' block" in w for w in warnings)

    def test_video_feature_missing_info(self, tmp_path):
        features = _make_info_json()["features"]
        features["observation.images.top"] = {"dtype": "video", "shape": [480, 640, 3]}
        ds = _make_valid_lerobot(tmp_path, {"features": features})
        # Also create videos dir to avoid that warning
        (ds / "videos" / "observation.images.top" / "chunk-000").mkdir(parents=True)
        (ds / "videos" / "observation.images.top" / "chunk-000" / "file-000.mp4").write_bytes(
            b"fake"
        )
        warnings = validate_lerobot(ds)
        assert any("Video feature" in w and "info" in w for w in warnings)

    def test_missing_video_directory(self, tmp_path):
        features = _make_info_json()["features"]
        features["observation.images.top"] = {
            "dtype": "video",
            "shape": [480, 640, 3],
            "info": {},
        }
        ds = _make_valid_lerobot(tmp_path, {"features": features})
        warnings = validate_lerobot(ds)
        assert any("videos/ directory is missing" in w for w in warnings)

    def test_missing_specific_video_feature_dir(self, tmp_path):
        features = _make_info_json()["features"]
        features["observation.images.top"] = {
            "dtype": "video",
            "shape": [480, 640, 3],
            "info": {},
        }
        ds = _make_valid_lerobot(tmp_path, {"features": features})
        (ds / "videos").mkdir()
        warnings = validate_lerobot(ds)
        assert any("videos/observation.images.top/" in w for w in warnings)

    def test_missing_stats_json(self, tmp_path):
        ds = _make_valid_lerobot(tmp_path)
        (ds / "meta" / "stats.json").unlink()
        warnings = validate_lerobot(ds)
        assert any("stats.json" in w for w in warnings)

    def test_missing_tasks_file(self, tmp_path):
        ds = _make_valid_lerobot(tmp_path)
        (ds / "meta" / "tasks.parquet").unlink()
        warnings = validate_lerobot(ds)
        assert any("tasks" in w for w in warnings)

    def test_missing_episodes_dir(self, tmp_path):
        ds = _make_valid_lerobot(tmp_path)
        import shutil

        shutil.rmtree(ds / "meta" / "episodes")
        warnings = validate_lerobot(ds)
        assert any("meta/episodes/" in w for w in warnings)

    def test_missing_data_dir(self, tmp_path):
        ds = _make_valid_lerobot(tmp_path)
        import shutil

        shutil.rmtree(ds / "data")
        warnings = validate_lerobot(ds)
        assert any("data/" in w for w in warnings)

    def test_data_wrong_chunk_naming(self, tmp_path):
        ds = _make_valid_lerobot(tmp_path)
        import shutil

        shutil.rmtree(ds / "data")
        bad_dir = ds / "data" / "wrong-name"
        bad_dir.mkdir(parents=True)
        (bad_dir / "episode_000000.parquet").write_bytes(b"PAR1")
        warnings = validate_lerobot(ds)
        assert any("chunk-NNN" in w for w in warnings)

    def test_missing_default_features(self, tmp_path):
        features = {
            "action": {"dtype": "float32", "shape": [6], "names": None},
        }
        ds = _make_valid_lerobot(tmp_path, {"features": features})
        warnings = validate_lerobot(ds)
        assert any("missing default features" in w for w in warnings)

    def test_stats_missing_feature_coverage(self, tmp_path):
        ds = _make_valid_lerobot(tmp_path)
        # Overwrite stats with empty
        (ds / "meta" / "stats.json").write_text(json.dumps({}))
        warnings = validate_lerobot(ds)
        assert any("stats.json missing stats" in w for w in warnings)

    def test_stats_missing_required_keys(self, tmp_path):
        ds = _make_valid_lerobot(tmp_path)
        # Stats missing 'count' (a required key)
        partial = {"min": [0], "max": [1], "mean": [0.5], "std": [0.1]}
        stats = {"action": partial, "observation.state": partial}
        (ds / "meta" / "stats.json").write_text(json.dumps(stats))
        warnings = validate_lerobot(ds)
        required_warnings = [w for w in warnings if "missing required stat keys" in w]
        assert len(required_warnings) == 2
        assert any("count" in w for w in required_warnings)

    def test_stats_missing_quantile_keys(self, tmp_path):
        ds = _make_valid_lerobot(tmp_path)
        # Stats with required keys but no quantiles
        base = {"min": [0], "max": [1], "mean": [0.5], "std": [0.1], "count": [100]}
        stats = {"action": base, "observation.state": base}
        (ds / "meta" / "stats.json").write_text(json.dumps(stats))
        warnings = validate_lerobot(ds)
        assert any("missing recommended quantile stats" in w for w in warnings)

    def test_stats_not_a_dict_entry(self, tmp_path):
        ds = _make_valid_lerobot(tmp_path)
        stats = {"action": "not a dict", "observation.state": {"min": [0]}}
        (ds / "meta" / "stats.json").write_text(json.dumps(stats))
        warnings = validate_lerobot(ds)
        assert any("'action' should be a dict" in w for w in warnings)


# ── MCAP validator tests ─────────────────────────────────────────────────────


class TestMcapValidator:
    def test_valid_dataset_no_warnings(self, tmp_path):
        ds = _make_valid_mcap_dataset(tmp_path)
        assert validate_mcap(ds) == []

    def test_no_mcap_files(self, tmp_path):
        ds = tmp_path / "empty"
        ds.mkdir()
        warnings = validate_mcap(ds)
        assert any("No .mcap files" in w for w in warnings)

    def test_single_file_validation(self, tmp_path):
        f = tmp_path / "episode_000000.mcap"
        _make_valid_mcap_file(f)
        warnings = validate_mcap(f)
        assert warnings == []

    def test_single_non_mcap_file(self, tmp_path):
        f = tmp_path / "data.bin"
        f.write_bytes(b"not mcap data at all, just random bytes 1234567890")
        warnings = validate_mcap(f)
        assert any(".mcap extension" in w for w in warnings)

    def test_invalid_magic_bytes(self, tmp_path):
        ds = tmp_path / "dataset"
        ds.mkdir()
        f = ds / "episode_000000.mcap"
        f.write_bytes(b"\x00" * 100)
        warnings = validate_mcap(ds)
        assert any("Invalid MCAP header magic" in w for w in warnings)

    def test_empty_file(self, tmp_path):
        ds = tmp_path / "dataset"
        ds.mkdir()
        f = ds / "episode_000000.mcap"
        f.write_bytes(b"")
        warnings = validate_mcap(ds)
        assert any("empty" in w for w in warnings)

    def test_too_small_file(self, tmp_path):
        ds = tmp_path / "dataset"
        ds.mkdir()
        f = ds / "episode_000000.mcap"
        f.write_bytes(b"\x89MCAP")
        warnings = validate_mcap(ds)
        assert any("too small" in w for w in warnings)

    def test_truncated_file_bad_footer(self, tmp_path):
        ds = tmp_path / "dataset"
        ds.mkdir()
        f = ds / "episode_000000.mcap"
        # Valid header magic but invalid footer
        data = MCAP_MAGIC + _write_mcap_header() + b"\x00" * 20 + b"\xff" * 8
        f.write_bytes(data)
        warnings = validate_mcap(ds)
        assert any("footer magic" in w for w in warnings)

    def test_wrong_profile(self, tmp_path):
        ds = tmp_path / "dataset"
        ds.mkdir()
        f = ds / "episode_000000.mcap"
        _make_valid_mcap_file(f, profile="ros2")
        warnings = validate_mcap(ds)
        assert any("ros2" in w and "trossen" in w for w in warnings)

    def test_non_conforming_filenames(self, tmp_path):
        ds = tmp_path / "dataset"
        ds.mkdir()
        _make_valid_mcap_file(ds / "recording_001.mcap")
        warnings = validate_mcap(ds)
        assert any("episode_NNNNNN.mcap" in w for w in warnings)

    def test_episode_index_gaps(self, tmp_path):
        ds = tmp_path / "dataset"
        ds.mkdir()
        for i in [0, 1, 3, 5]:  # gap at 2 and 4
            _make_valid_mcap_file(ds / f"episode_{i:06d}.mcap")
        warnings = validate_mcap(ds)
        assert any("Missing episode indices" in w or "gaps" in w for w in warnings)

    def test_episode_consecutive_no_gap_warning(self, tmp_path):
        ds = _make_valid_mcap_dataset(tmp_path, num_episodes=5)
        warnings = validate_mcap(ds)
        gap_warnings = [w for w in warnings if "gap" in w.lower() or "Missing episode" in w]
        assert gap_warnings == []

    def test_many_non_conforming_names_truncated(self, tmp_path):
        ds = tmp_path / "dataset"
        ds.mkdir()
        for i in range(10):
            _make_valid_mcap_file(ds / f"bad_name_{i}.mcap")
        warnings = validate_mcap(ds)
        assert any("10 MCAP files not matching" in w for w in warnings)

    def test_many_gaps_truncated(self, tmp_path):
        ds = tmp_path / "dataset"
        ds.mkdir()
        # 12 even-indexed episodes → expected range {0..11}, gaps = {1,3,5,7,9,11} = 6
        for i in range(0, 24, 2):
            _make_valid_mcap_file(ds / f"episode_{i:06d}.mcap")
        warnings = validate_mcap(ds)
        assert any("gaps in episode numbering" in w for w in warnings)

    def test_gap_detection_min_to_max(self, tmp_path):
        """Gaps are computed from min to max index, not min + count."""
        ds = tmp_path / "dataset"
        ds.mkdir()
        for i in [0, 1, 3, 5]:  # missing 2 and 4
            _make_valid_mcap_file(ds / f"episode_{i:06d}.mcap")
        warnings = validate_mcap(ds)
        gap_warnings = [w for w in warnings if "Missing episode" in w]
        assert len(gap_warnings) == 1
        assert "2" in gap_warnings[0]
        assert "4" in gap_warnings[0]


# ── LeRobot robustness tests ────────────────────────────────────────────────


class TestLeRobotRobustness:
    def test_nonexistent_path(self, tmp_path):
        warnings = validate_lerobot(tmp_path / "does_not_exist")
        assert any("does not exist" in w for w in warnings)

    def test_non_dict_feature_in_stats_validation(self, tmp_path):
        """Non-dict feature entries don't crash stats validation."""
        features = _make_info_json()["features"]
        features["bad"] = "not a dict"
        ds = _make_valid_lerobot(tmp_path, {"features": features})
        warnings = validate_lerobot(ds)
        # Should warn about the bad feature but not crash
        assert any("bad" in w and "should be a dict" in w for w in warnings)

    def test_chunk_dir_requires_numeric_suffix(self, tmp_path):
        """chunk-abc should be flagged as non-conforming."""
        ds = _make_valid_lerobot(tmp_path)
        import shutil

        shutil.rmtree(ds / "data")
        bad_dir = ds / "data" / "chunk-abc"
        bad_dir.mkdir(parents=True)
        (bad_dir / "episode_000000.parquet").write_bytes(b"PAR1")
        warnings = validate_lerobot(ds)
        assert any("chunk-NNN" in w for w in warnings)


# ── --force flag tests ───────────────────────────────────────────────────────

MOCK_TOKEN = "tr_test_token_1234567890abcdef"


class TestForceFlag:
    def test_upload_force_skips_confirmation(self, tmp_path):
        """--force bypasses the validation confirmation prompt."""
        ds = _make_valid_mcap_dataset(tmp_path)
        upload_result = {"id": "ds-123", "name": "test"}
        with (
            patch("trossen_cloud_cli.auth.get_token", return_value=MOCK_TOKEN),
            patch(
                "trossen_cloud_cli.commands.datasets.validate_dataset",
                return_value=["some warning"],
            ),
            patch(
                "trossen_cloud_cli.commands.datasets.create_and_upload_dataset",
                return_value=upload_result,
            ) as upload_mock,
        ):
            result = runner.invoke(
                app,
                ["dataset", "upload", str(ds), "--name", "test", "--type", "mcap", "--force"],
            )
            assert result.exit_code == 0
            upload_mock.assert_called_once()

    def test_upload_no_force_prompts_and_aborts(self, tmp_path):
        """Without --force, validation warnings trigger a prompt; 'n' aborts."""
        ds = _make_valid_mcap_dataset(tmp_path)
        with (
            patch("trossen_cloud_cli.auth.get_token", return_value=MOCK_TOKEN),
            patch(
                "trossen_cloud_cli.commands.datasets.validate_dataset",
                return_value=["some warning"],
            ),
            patch(
                "trossen_cloud_cli.commands.datasets.create_and_upload_dataset",
            ) as upload_mock,
        ):
            result = runner.invoke(
                app,
                ["dataset", "upload", str(ds), "--name", "test", "--type", "mcap"],
                input="n\n",
            )
            assert result.exit_code == 0
            upload_mock.assert_not_called()
