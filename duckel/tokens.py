"""
Token resolution for connection strings: `__ENV:VAR` and `SECRET:NAME`.

This module sits below the adapters and the configuration models. An adapter resolves a token
at attach time, and the models validate a configuration against the adapters, so neither may
import the other's side of `duckel/config.py`.
"""

import os
import re

from dotenv import load_dotenv

from .logger import logger

# Load environment variables from .env file if it exists
load_dotenv()


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
            logger.error(f"Environment variable {var} not found")
            raise ValueError(f"Required environment variable '{var}' is not set.")
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
            logger.error(f"Secret {secret_name} not found in environment")
            raise ValueError(f"Required secret '{secret_name}' is not set.")

        # If the entire string is just the token, return the value directly (to preserve type if possible, though here it's still string)
        if s == token:
            return value
        result = result.replace(token, value)

    return result
