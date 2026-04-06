"""Trossen MCAP dataset validator."""

import re
from pathlib import Path

MCAP_MAGIC = bytes([0x89, 0x4D, 0x43, 0x41, 0x50, 0x30, 0x0D, 0x0A])
EPISODE_PATTERN = re.compile(r"^episode_(\d{6})\.mcap$")


def validate_mcap(path: Path) -> list[str]:
    """
    Validate a Trossen MCAP dataset directory structure and file integrity.

    Returns a list of warning strings describing spec drift.
    """
    warnings: list[str] = []

    if path.is_file():
        return _validate_single_mcap(path, warnings)

    if not path.is_dir():
        warnings.append(f"Path does not exist: {path}")
        return warnings

    # Find all .mcap files
    mcap_files = sorted(path.rglob("*.mcap"))
    if not mcap_files:
        warnings.append("No .mcap files found in dataset directory")
        return warnings

    # Check episode naming convention
    _validate_episode_naming(mcap_files, path, warnings)

    # Validate each MCAP file (magic bytes + basic structure)
    for mcap_file in mcap_files:
        _validate_mcap_file(mcap_file, path, warnings)

    return warnings


def _validate_single_mcap(path: Path, warnings: list[str]) -> list[str]:
    """Validate a single .mcap file."""
    if not path.name.endswith(".mcap"):
        warnings.append(f"File does not have .mcap extension: {path.name}")

    _validate_mcap_file(path, path.parent, warnings)
    return warnings


def _validate_episode_naming(mcap_files: list[Path], base_dir: Path, warnings: list[str]) -> None:
    """Check that MCAP files follow the episode_NNNNNN.mcap naming convention."""
    episode_indices: list[int] = []
    non_conforming: list[str] = []

    for f in mcap_files:
        match = EPISODE_PATTERN.match(f.name)
        if match:
            episode_indices.append(int(match.group(1)))
        else:
            non_conforming.append(f.name)

    if non_conforming:
        if len(non_conforming) <= 5:
            warnings.append(
                "MCAP files not matching episode_NNNNNN.mcap convention: "
                + ", ".join(non_conforming)
            )
        else:
            warnings.append(
                f"{len(non_conforming)} MCAP files not matching episode_NNNNNN.mcap convention"
            )

    # Check for gaps in episode indices
    if episode_indices:
        episode_indices.sort()
        # Get the expected range of episode indices based on min and max found
        expected = set(range(episode_indices[0], episode_indices[-1] + 1))
        actual = set(episode_indices)
        gaps = sorted(expected - actual)
        if gaps:
            if len(gaps) <= 5:
                warnings.append(f"Missing episode indices: {gaps}")
            else:
                warnings.append(
                    f"Found {len(gaps)} gaps in episode numbering "
                    f"(range {episode_indices[0]}-{episode_indices[-1]}, "
                    f"have {len(episode_indices)} files)"
                )


def _validate_mcap_file(path: Path, base_dir: Path, warnings: list[str]) -> None:
    """Validate a single MCAP file's magic bytes and basic structure."""
    rel = path.relative_to(base_dir) if path != base_dir else Path(path.name)

    # Check file size
    try:
        size = path.stat().st_size
    except OSError as e:
        warnings.append(f"{rel}: Cannot read file - {e}")
        return

    if size == 0:
        warnings.append(f"{rel}: File is empty")
        return

    if size < 16:
        warnings.append(f"{rel}: File too small to be valid MCAP ({size} bytes)")
        return

    # Check leading magic bytes
    try:
        with open(path, "rb") as f:
            header = f.read(8)
            if header != MCAP_MAGIC:
                warnings.append(
                    f"{rel}: Invalid MCAP header magic bytes "
                    f"(got {header.hex()}, expected {MCAP_MAGIC.hex()})"
                )
                return

            # Check trailing magic bytes
            f.seek(-8, 2)
            footer = f.read(8)
            if footer != MCAP_MAGIC:
                warnings.append(f"{rel}: Invalid MCAP footer magic bytes - file may be truncated")

            # Read the first record after magic to check profile
            f.seek(8)
            _check_header_record(f, rel, warnings)

    except OSError as e:
        warnings.append(f"{rel}: Error reading file - {e}")


def _check_header_record(f, rel: Path, warnings: list[str]) -> None:
    """Check the MCAP header record for profile information."""
    try:
        opcode = f.read(1)
        if not opcode:
            return
        if opcode[0] != 0x01:
            warnings.append(f"{rel}: First record is not a Header (opcode 0x{opcode[0]:02x})")
            return

        # Read content length (uint64 LE)
        length_bytes = f.read(8)
        if len(length_bytes) < 8:
            return
        content_length = int.from_bytes(length_bytes, "little")

        if content_length > 1024 * 1024:  # Sanity check: header shouldn't exceed 1MB
            return

        content = f.read(content_length)
        if len(content) < content_length:
            return

        # Parse profile string (prefixed uint32 length + utf8 bytes)
        profile, _ = _read_prefixed_string(content, 0)
        if profile is not None and profile != "trossen":
            warnings.append(f"{rel}: MCAP profile is '{profile}' (expected 'trossen')")

    except Exception:
        pass  # Don't warn on parse failures - magic bytes check is sufficient


def _read_prefixed_string(data: bytes, offset: int) -> tuple[str | None, int]:
    """Read a uint32-prefixed string from bytes."""
    if offset + 4 > len(data):
        return None, offset
    str_len = int.from_bytes(data[offset : offset + 4], "little")
    offset += 4
    if offset + str_len > len(data):
        return None, offset
    return data[offset : offset + str_len].decode("utf-8", errors="replace"), offset + str_len
