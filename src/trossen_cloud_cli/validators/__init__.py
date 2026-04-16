"""Dataset validators and type detection for pre-upload structural checks."""

import os
from pathlib import Path

from ..types import DatasetType
from .lerobot import validate_lerobot
from .mcap import validate_mcap


def _has_visible_mcap(root: Path) -> bool:
    """True if ``root`` contains a non-hidden ``.mcap`` file outside any hidden directory.

    Hidden subdirectories (e.g. ``.git``, ``.cache``) are pruned during traversal
    rather than walked-then-filtered, so detection stays fast on trees that
    contain large hidden directories.
    """
    for _dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if not d.startswith(".")]
        if any(name.endswith(".mcap") and not name.startswith(".") for name in filenames):
            return True
    return False


def detect_dataset_type(path: Path) -> DatasetType | None:
    """
    Detect the dataset type from its contents.

    Returns the detected DatasetType, or None if the type cannot be determined.
    Hidden filenames (those starting with ``.``) are ignored. The treatment of
    parent directories depends on the input shape, mirroring ``collect_files``:

    * **Directory input:** hidden subdirectories (e.g. ``.git``, ``.cache``)
      are skipped during traversal.
    * **Single-file input** (e.g. passing ``some/.cache/foo.mcap`` directly):
      only the filename is checked — parent directory names don't matter,
      since ``collect_files`` would still upload that file.
    """
    if path.is_file() and path.suffix == ".mcap" and not path.name.startswith("."):
        return DatasetType.TROSSENMCAP
    if path.is_dir():
        if (path / "meta" / "info.json").is_file():
            return DatasetType.LEROBOT_V3
        if _has_visible_mcap(path):
            return DatasetType.TROSSENMCAP
    return None


def validate_dataset(path: Path, dataset_type: DatasetType) -> list[str]:
    """
    Validate a dataset directory against its type-specific spec.

    Returns a list of warning messages. An empty list means no issues found.
    Only runs for dataset types that have a validator (trossenmcap, lerobot_v3).
    """
    validators = {
        DatasetType.TROSSENMCAP: validate_mcap,
        DatasetType.LEROBOT_V3: validate_lerobot,
    }

    validator = validators.get(dataset_type)
    if validator is None:
        return []

    return validator(path)
