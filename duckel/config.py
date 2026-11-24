"""
Configuration loading and validation using Pydantic.
"""

import json
import re
from pathlib import Path

import yaml

from .errors import ConfigError
from .logger import logger
from .models import PipelineConfig
from .tokens import resolve_env_tokens, resolve_secret_tokens

__all__ = [
    "load_config",
    "resolve_env_tokens",
    "resolve_secret_tokens",
    "resolve_tokens_in_dict",
    "save_pipeline_config",
]


def resolve_tokens_in_dict(d: dict) -> dict:
    """
    Recursively resolve environment and secret tokens in a dictionary.

    Args:
        d: Dictionary potentially containing token strings

    Returns:
        Dictionary with all tokens resolved
    """
    result = {}
    for key, value in d.items():
        if isinstance(value, str):
            value = resolve_env_tokens(value)
            value = resolve_secret_tokens(value)
        elif isinstance(value, dict):
            value = resolve_tokens_in_dict(value)
        result[key] = value
    return result


def load_config(path: str) -> dict[str, PipelineConfig]:
    """
    Load and validate pipeline configuration from YAML file.

    Args:
        path: Path to YAML configuration file

    Returns:
        Dictionary mapping pipeline names to validated PipelineConfig objects

    Raises:
        ConfigError: If a pipeline names something its source or target cannot do
        ValueError: If configuration is invalid
        FileNotFoundError: If config file doesn't exist
    """
    config_path = Path(path)
    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {path}")

    logger.info(f"Loading configuration from {path}")

    with open(path, encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    if "pipelines" not in cfg:
        raise ValueError("Missing top-level 'pipelines' key in configuration")

    _refuse_plain_text_passwords(cfg)

    # Resolve environment variables and secrets
    cfg = resolve_tokens_in_dict(cfg)

    # Validate each pipeline configuration
    validated_pipelines = {
        name: _validated_pipeline(name, pipeline_dict)
        for name, pipeline_dict in cfg["pipelines"].items()
    }

    logger.info(f"Loaded {len(validated_pipelines)} pipelines")
    return validated_pipelines


def _refuse_plain_text_passwords(cfg: dict) -> None:
    """Raise ValueError when a configuration spells a password out in place of a token."""
    raw_str = json.dumps(cfg)
    if re.search(r"(?i)\b(?:password|pwd)\s*=\s*(?!__ENV:|SECRET:)", raw_str):
        raise ValueError(
            "Plain-text passwords are not allowed in configurations. Use __ENV:VAR or SECRET:VAR instead."
        )


def _validated_pipeline(name: str, pipeline_dict: dict) -> PipelineConfig:
    """One pipeline as a validated PipelineConfig. A refusal carries the pipeline's name."""
    try:
        validated = PipelineConfig(**pipeline_dict)
        logger.debug(f"Validated pipeline: {name}")
        return validated
    except ConfigError as e:
        logger.error(f"Configuration for pipeline '{name}' is refused: {e}")
        raise ConfigError(e.category, f"Pipeline '{name}': {e.message}", e.hint) from e
    except Exception as e:
        logger.error(f"Invalid configuration for pipeline '{name}': {e}")
        raise ValueError(f"Invalid configuration for pipeline '{name}': {e}") from e


def save_pipeline_config(path: str, name: str, config: PipelineConfig) -> None:
    """
    Save a pipeline configuration to the YAML file.

    Args:
        path: Path to YAML configuration file
        name: Name of the pipeline
        config: PipelineConfig object to save
    """
    config_path = Path(path)

    # Read existing config
    if config_path.exists():
        with open(path, encoding="utf-8") as f:
            cfg = yaml.safe_load(f) or {}
    else:
        cfg = {"pipelines": {}}

    if "pipelines" not in cfg:
        cfg["pipelines"] = {}

    # Update config
    # We dump the model to dict, excluding defaults to keep it clean
    pipeline_dict = config.model_dump(exclude_defaults=True)
    cfg["pipelines"][name] = pipeline_dict

    # Write back
    with open(path, "w", encoding="utf-8") as f:
        yaml.dump(cfg, f, sort_keys=False, indent=2)

    logger.info(f"Saved pipeline '{name}' to {path}")
