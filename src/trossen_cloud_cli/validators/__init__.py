"""Dataset validators for pre-upload structural checks."""

from pathlib import Path

from ..types import DatasetType
from .lerobot import validate_lerobot
from .mcap import validate_mcap


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
