"""
Configuration loading and validation using Pydantic.
"""
import os
import yaml
from typing import Dict
from pathlib import Path
from dotenv import load_dotenv
from .models import PipelineConfig
from .logger import logger

# Load environment variables from .env file if it exists
load_dotenv()


import re

def resolve_env_tokens(s: str) -> str:
    """
    Replace __ENV:VAR tokens with environment variable values.
    
    Args:
        s: String potentially containing __ENV:VAR tokens
        
    Returns:
        String with environment variables resolved
    """
    if not isinstance(s, str):
        return s
    
    pattern = r"__ENV:([A-Z0-9_]+)"
    matches = re.finditer(pattern, s)
    
    result = s
    for match in matches:
        token = match.group(0)
        var = match.group(1)
        value = os.environ.get(var, "")
        if not value:
            logger.warning(f"Environment variable {var} not found")
        result = result.replace(token, value)
    
    return result


def resolve_secret_tokens(s: str) -> str:
    """
    Replace SECRET:NAME tokens with environment variable values.
    
    This is a placeholder for future secret manager integration.
    Currently resolves from environment variables.
    
    Args:
        s: String potentially containing SECRET:NAME tokens
        
    Returns:
        String with secrets resolved
    """
    if not isinstance(s, str):
        return s
    
    pattern = r"SECRET:([A-Z0-9_]+)"
    matches = re.finditer(pattern, s)
    
    result = s
    for match in matches:
        token = match.group(0)
        secret_name = match.group(1)
        # TODO: Integrate with AWS Secrets Manager / Azure Key Vault
        value = os.environ.get(secret_name, "")
        if not value:
            logger.warning(f"Secret {secret_name} not found in environment")
        
        # If the entire string is just the token, return the value directly (to preserve type if possible, though here it's still string)
        if s == token:
            return value
        result = result.replace(token, value)
    
    return result


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


def load_config(path: str) -> Dict[str, PipelineConfig]:
    """
    Load and validate pipeline configuration from YAML file.
    
    Args:
        path: Path to YAML configuration file
        
    Returns:
        Dictionary mapping pipeline names to validated PipelineConfig objects
        
    Raises:
        ValueError: If configuration is invalid
        FileNotFoundError: If config file doesn't exist
    """
    config_path = Path(path)
    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {path}")
    
    logger.info(f"Loading configuration from {path}")
    
    with open(path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    
    if "pipelines" not in cfg:
        raise ValueError("Missing top-level 'pipelines' key in configuration")
    
    # Resolve environment variables and secrets
    cfg = resolve_tokens_in_dict(cfg)
    
    # Validate each pipeline configuration
    validated_pipelines = {}
    for name, pipeline_dict in cfg["pipelines"].items():
        try:
            validated_pipelines[name] = PipelineConfig(**pipeline_dict)
            logger.debug(f"Validated pipeline: {name}")
        except Exception as e:
            logger.error(f"Invalid configuration for pipeline '{name}': {e}")
            raise ValueError(f"Invalid configuration for pipeline '{name}': {e}") from e
    
    logger.info(f"Loaded {len(validated_pipelines)} pipelines")
    return validated_pipelines


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
        with open(path, "r", encoding="utf-8") as f:
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

