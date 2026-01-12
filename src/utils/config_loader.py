# src/utils/config_loader.py

import yaml
from pathlib import Path


class ConfigError(Exception):
    """Raised when configuration is invalid or missing."""
    pass


def load_yaml_config(template_path: str, local_path: str | None = None) -> dict:
    """
    Load YAML config safely.
    Priority:
      1. local config (secrets)
      2. template config (fallback)

    :param template_path: Path to template YAML (safe)
    :param local_path: Path to local YAML (secrets)
    :return: config dictionary
    """

    template_file = Path(template_path)
    local_file = Path(local_path) if local_path else None

    if local_file and local_file.exists():
        with local_file.open("r") as f:
            config = yaml.safe_load(f)
    elif template_file.exists():
        with template_file.open("r") as f:
            config = yaml.safe_load(f)
    else:
        raise ConfigError(
            f"No config found. Checked: {template_path} and {local_path}"
        )

    if not isinstance(config, dict):
        raise ConfigError("Invalid YAML format: root must be a dictionary")

    return config
