"""Configuration management for TRC CLI."""
import json
import os
from pathlib import Path
from typing import Dict, Any, Optional


CONFIG_DIR = Path.home() / ".trossen"
CONFIG_FILE = CONFIG_DIR / "config.json"


def ensure_config_dir() -> None:
    """Create the config directory if it doesn't exist."""
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)


def load_config() -> Dict[str, Any]:
    """
    Load configuration from the config file.
    
    :returns: Dictionary containing configuration data, or empty dict if file doesn't exist.
    :rtype: Dict[str, Any]
    """
    if not CONFIG_FILE.exists():
        return {}
    
    try:
        with open(CONFIG_FILE, 'r') as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError):
        return {}


def save_config(config: Dict[str, Any]) -> None:
    """
    Save configuration to the config file.
    
    :param config: Dictionary containing configuration data to save.
    :type config: Dict[str, Any]
    """
    ensure_config_dir()
    with open(CONFIG_FILE, 'w') as f:
        json.dump(config, f, indent=2)


def clear_config() -> None:
    """Clear the configuration file by deleting it."""
    if CONFIG_FILE.exists():
        CONFIG_FILE.unlink()
