"""
Typed errors for a configuration that cannot be honoured.

Each error carries a category, a message and a hint that names a fix that exists.
"""

from enum import Enum


class ConfigErrorCategory(str, Enum):
    """Why a configuration is refused."""

    UNSUPPORTED_MODE = "unsupported_mode"


class ConfigError(Exception):
    """
    A configuration refused when it loads, before any run.

    It is not a `ValueError` on purpose: pydantic wraps a `ValueError` raised in a validator
    into its own `ValidationError`, and the category and the hint would be lost.

    Attributes:
        category: Why the configuration is refused
        message: What is wrong, naming the setting and its value
        hint: What to write instead
    """

    def __init__(self, category: ConfigErrorCategory, message: str, hint: str):
        super().__init__(f"[{category.value}] {message} {hint}")
        self.category = category
        self.message = message
        self.hint = hint
