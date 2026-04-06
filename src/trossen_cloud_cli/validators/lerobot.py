"""LeRobot v3 dataset validator."""

import json
import re
from pathlib import Path

REQUIRED_INFO_FIELDS = {
    "codebase_version",
    "robot_type",
    "total_episodes",
    "total_frames",
    "total_tasks",
    "fps",
    "features",
}

RECOMMENDED_INFO_FIELDS = {
    "chunks_size",
    "data_path",
    "splits",
    "data_files_size_in_mb",
    "video_files_size_in_mb",
}

DEFAULT_FEATURE_KEYS = {"timestamp", "frame_index", "episode_index", "index", "task_index"}

VALID_DTYPES = {
    "float16",
    "float32",
    "float64",
    "int8",
    "int16",
    "int32",
    "int64",
    "uint8",
    "bool",
    "image",
    "video",
    "string",
}

REQUIRED_STAT_KEYS = {"min", "max", "mean", "std", "count"}
RECOMMENDED_STAT_KEYS = {"q01", "q10", "q50", "q90", "q99"}


def validate_lerobot(path: Path) -> list[str]:
    """
    Validate a LeRobot v3 dataset directory structure and metadata.

    Returns a list of warning strings describing spec drift.
    """
    warnings: list[str] = []

    if not path.exists():
        warnings.append(f"Path does not exist: {path}")
        return warnings
    if not path.is_dir():
        warnings.append(f"Expected a directory but got a file: {path.name}")
        return warnings

    # -- meta/info.json --
    info_path = path / "meta" / "info.json"
    if not info_path.is_file():
        warnings.append("Missing required file: meta/info.json")
        return warnings  # Can't validate further without info.json

    info = _load_json(info_path, warnings)
    if info is None:
        return warnings

    _validate_info(info, warnings)
    features = info.get("features", {})
    if not isinstance(features, dict):
        features = {}
    video_features = {
        k for k, v in features.items() if isinstance(v, dict) and v.get("dtype") == "video"
    }

    # -- meta/stats.json (required for training - used for normalization) --
    stats_path = path / "meta" / "stats.json"
    if not stats_path.is_file():
        warnings.append("Missing required file: meta/stats.json (needed for feature normalization)")
    else:
        stats = _load_json(stats_path, warnings)
        if stats is not None:
            _validate_stats_keys(stats, features, warnings)

    # -- meta/tasks (parquet or jsonl) --
    tasks_parquet = path / "meta" / "tasks.parquet"
    tasks_jsonl = path / "meta" / "tasks.jsonl"
    if not tasks_parquet.is_file() and not tasks_jsonl.is_file():
        warnings.append("Missing required file: meta/tasks.parquet (or meta/tasks.jsonl)")

    # -- meta/episodes/ --
    episodes_dir = path / "meta" / "episodes"
    if not episodes_dir.is_dir():
        warnings.append("Missing required directory: meta/episodes/")
    else:
        episode_files = list(episodes_dir.rglob("*.parquet"))
        if not episode_files:
            warnings.append("No parquet files found in meta/episodes/")

    # -- data/ --
    data_dir = path / "data"
    if not data_dir.is_dir():
        warnings.append("Missing required directory: data/")
    else:
        data_files = list(data_dir.rglob("*.parquet"))
        if not data_files:
            warnings.append("No parquet files found in data/")
        else:
            _validate_chunk_structure(data_files, data_dir, "data", warnings)

    # -- videos/ --
    if video_features:
        # video_path template is needed to locate video files
        if "video_path" not in info:
            warnings.append(
                "meta/info.json missing 'video_path' template "
                "(required when video features are defined)"
            )

        videos_dir = path / "videos"
        if not videos_dir.is_dir():
            warnings.append(
                f"Features declare video dtype ({', '.join(sorted(video_features))}) "
                "but videos/ directory is missing"
            )
        else:
            for vf in sorted(video_features):
                vf_dir = videos_dir / vf
                if not vf_dir.is_dir():
                    warnings.append(f"Missing video directory for feature: videos/{vf}/")
                else:
                    video_files = list(vf_dir.rglob("*.mp4"))
                    if not video_files:
                        warnings.append(f"No .mp4 files found in videos/{vf}/")

    return warnings


def _load_json(path: Path, warnings: list[str]) -> dict | None:
    try:
        with open(path) as f:
            data = json.load(f)
        if not isinstance(data, dict):
            warnings.append(f"{path.name}: Expected a JSON object, got {type(data).__name__}")
            return None
        return data
    except json.JSONDecodeError as e:
        warnings.append(f"{path.name}: Invalid JSON - {e}")
        return None


def _validate_info(info: dict, warnings: list[str]) -> None:
    # Required fields
    missing = REQUIRED_INFO_FIELDS - info.keys()
    if missing:
        warnings.append(f"meta/info.json missing required fields: {', '.join(sorted(missing))}")

    # Version check
    version = info.get("codebase_version", "")
    if version and not version.startswith("v3"):
        warnings.append(
            f"meta/info.json codebase_version is '{version}' (expected v3.x) - "
            "this may not be a LeRobot v3 dataset"
        )

    # Numeric field checks
    for field in ("total_episodes", "total_frames", "total_tasks", "fps"):
        val = info.get(field)
        if val is not None and (not isinstance(val, (int, float)) or val < 0):
            warnings.append(f"meta/info.json {field} should be a non-negative number, got: {val}")

    if info.get("fps") is not None and info["fps"] == 0:
        warnings.append("meta/info.json fps is 0 - this is likely incorrect")

    # Features validation
    features = info.get("features")
    if isinstance(features, dict):
        _validate_features(features, warnings)

    # Recommended fields
    recommended_missing = RECOMMENDED_INFO_FIELDS - info.keys()
    if recommended_missing:
        warnings.append(
            f"meta/info.json missing recommended fields: {', '.join(sorted(recommended_missing))}"
        )


def _validate_features(features: dict, warnings: list[str]) -> None:
    if not features:
        warnings.append("meta/info.json features dict is empty")
        return

    # Check default features exist
    missing_defaults = DEFAULT_FEATURE_KEYS - features.keys()
    if missing_defaults:
        warnings.append(
            f"meta/info.json missing default features: {', '.join(sorted(missing_defaults))}"
        )

    # Check for action feature (required by all training policies)
    if "action" not in features:
        warnings.append(
            "meta/info.json missing 'action' feature "
            "(required for training with pi0, pi0.5, smolvla)"
        )

    for name, feat in features.items():
        if not isinstance(feat, dict):
            warnings.append(f"Feature '{name}' should be a dict, got {type(feat).__name__}")
            continue

        # dtype validation
        dtype = feat.get("dtype")
        if dtype is None:
            warnings.append(f"Feature '{name}' missing required field 'dtype'")
        elif dtype not in VALID_DTYPES:
            warnings.append(f"Feature '{name}' has unrecognized dtype '{dtype}'")

        # shape validation
        shape = feat.get("shape")
        if shape is None:
            warnings.append(f"Feature '{name}' missing required field 'shape'")
        elif not isinstance(shape, list):
            warnings.append(f"Feature '{name}' shape should be a list, got {type(shape).__name__}")
        elif not all(isinstance(d, int) and d > 0 for d in shape):
            warnings.append(f"Feature '{name}' shape must contain only positive integers: {shape}")

        if "/" in name:
            warnings.append(f"Feature name '{name}' contains '/' which is not allowed")

        # Video features should have info block (accepts 'info' or 'video_info')
        if feat.get("dtype") == "video" and "info" not in feat and "video_info" not in feat:
            warnings.append(f"Video feature '{name}' missing 'info' block with video metadata")


def _validate_stats_keys(stats: dict, features: dict, warnings: list[str]) -> None:
    feature_keys = {
        k
        for k, v in features.items()
        if isinstance(v, dict) and v.get("dtype") not in ("video", "image")
    }
    stats_keys = set(stats.keys())

    # Stats should cover non-video/image features (at minimum)
    missing_stats = feature_keys - stats_keys - DEFAULT_FEATURE_KEYS
    if missing_stats:
        warnings.append(
            f"meta/stats.json missing stats for features: {', '.join(sorted(missing_stats))}"
        )

    # Check each stat entry has the required and recommended keys
    missing_quantiles: list[str] = []
    for feat_name, feat_stats in stats.items():
        if not isinstance(feat_stats, dict):
            warnings.append(f"meta/stats.json '{feat_name}' should be a dict")
            continue
        missing_required = REQUIRED_STAT_KEYS - feat_stats.keys()
        if missing_required:
            warnings.append(
                f"meta/stats.json '{feat_name}' missing required stat keys: "
                f"{', '.join(sorted(missing_required))}"
            )
        missing_recommended = RECOMMENDED_STAT_KEYS - feat_stats.keys()
        if missing_recommended:
            missing_quantiles.append(feat_name)

    if missing_quantiles:
        if len(missing_quantiles) <= 3:
            names = ", ".join(missing_quantiles)
        else:
            names = f"{len(missing_quantiles)} features"
        warnings.append(
            f"meta/stats.json missing recommended quantile stats (q01-q99) for {names} "
            "- may not be compatible with pi0/pi0.5 training pipelines. "
            "See: https://github.com/huggingface/lerobot/blob/main/src/"
            "lerobot/scripts/augment_dataset_quantile_stats.py"
        )


def _validate_chunk_structure(
    files: list[Path], base_dir: Path, label: str, warnings: list[str]
) -> None:
    """Check that parquet files live inside chunk-NNN/ directories."""
    non_conforming = []
    chunk_re = re.compile(r"^chunk-\d+$")
    for f in files:
        rel = f.relative_to(base_dir)
        parts = rel.parts
        if len(parts) != 2:
            non_conforming.append(str(rel))
            continue
        chunk_dir, _ = parts
        if not chunk_re.match(chunk_dir):
            non_conforming.append(str(rel))

    convention = "chunk-NNN/"
    if non_conforming and len(non_conforming) <= 5:
        warnings.append(
            f"{label}/ contains files not matching {convention} convention: "
            + ", ".join(non_conforming)
        )
    elif non_conforming:
        warnings.append(
            f"{label}/ has {len(non_conforming)} files not matching {convention} convention"
        )
