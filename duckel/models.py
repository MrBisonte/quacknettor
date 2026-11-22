"""
Pydantic models for pipeline configuration validation.

Provides type-safe configuration with automatic validation.
"""

import re
from typing import Any, Literal, Optional

from pydantic import BaseModel, Field, field_validator, model_validator

from .adapters import SOURCES, TARGETS, WriteMode, require_mode

# The validated names are the keys of the registries in `duckel/adapters.py`: one list.
# A source or target type with no adapter is refused here, when the configuration loads.
SourceType = Literal[tuple(SOURCES)]
TargetType = Literal[tuple(TARGETS)]
WriteModeName = Literal[tuple(mode.value for mode in WriteMode)]


class SourceConfig(BaseModel):
    """Configuration for data source."""

    type: SourceType
    path: Optional[str] = None
    conn: Optional[str] = None
    name: Optional[str] = None
    object: Optional[str] = None
    query: Optional[str] = None

    @field_validator("path")
    @classmethod
    def validate_path_for_file_sources(cls, v: Optional[str], info) -> Optional[str]:
        """Validate that file-based sources have a path."""
        if info.data.get("type") in ["parquet", "csv"] and not v:
            raise ValueError(f"path required for {info.data.get('type')} source")
        return v

    @field_validator("conn")
    @classmethod
    def validate_conn_for_db_sources(cls, v: Optional[str], info) -> Optional[str]:
        """Validate that database sources have a connection string."""
        if info.data.get("type") in ["postgres", "snowflake"] and not v:
            raise ValueError(f"conn required for {info.data.get('type')} source")
        return v

    @field_validator("object", "name")
    @classmethod
    def sanitize_identifier(cls, v: Optional[str]) -> Optional[str]:
        """Sanitize SQL identifiers to prevent injection."""
        if v and not re.match(r"^[a-zA-Z0-9_\.]+$", v):
            raise ValueError(
                f"Invalid SQL identifier: {v}. Only alphanumeric, underscore, and dot allowed."
            )
        return v

    # Incremental loading options
    incremental_key: Optional[str] = None
    lookback_window: Optional[str] = None


class TargetConfig(BaseModel):
    """Configuration for data target."""

    type: TargetType
    path: Optional[str] = None
    conn: Optional[str] = None
    name: Optional[str] = None
    table: Optional[str] = None
    # None means the configuration names no mode: the target then gets the default it declares.
    mode: Optional[WriteModeName] = None
    compression: Optional[str] = "zstd"

    # Evolution and upsert options
    unique_key: Optional[str] = None
    schema_evolution: Literal["ignore", "fail", "evolve"] = "ignore"

    @field_validator("path")
    @classmethod
    def validate_path_for_file_targets(cls, v: Optional[str], info) -> Optional[str]:
        """Validate that file-based targets have a path."""
        if info.data.get("type") in ["parquet", "csv"] and not v:
            raise ValueError(f"path required for {info.data.get('type')} target")
        return v

    @field_validator("conn")
    @classmethod
    def validate_conn_for_db_targets(cls, v: Optional[str], info) -> Optional[str]:
        """Validate that database targets have a connection string."""
        if info.data.get("type") in ["postgres", "snowflake"] and not v:
            raise ValueError(f"conn required for {info.data.get('type')} target")
        return v

    @field_validator("table")
    @classmethod
    def validate_table_for_db_targets(cls, v: Optional[str], info) -> Optional[str]:
        """Validate that database targets have a table name if not in testing mode."""
        # We allow table to be None initially for connection testing in the UI
        return v

    @field_validator("table", "name", "unique_key")
    @classmethod
    def sanitize_identifier(cls, v: Optional[str]) -> Optional[str]:
        """Sanitize SQL identifiers to prevent injection."""
        if v and not re.match(r"^[a-zA-Z0-9_\.,]+$", v):  # Added comma support for composite keys
            raise ValueError(
                f"Invalid SQL identifier: {v}. Only alphanumeric, underscore, dot, and comma allowed."
            )
        return v

    @model_validator(mode="after")
    def settle_the_mode(self) -> "TargetConfig":
        """
        Give a target with no mode the default it declares, and refuse a mode it does not declare.

        Raises:
            ConfigError: If the target does not declare the mode
        """
        adapter = TARGETS[self.type]
        named = self.mode or adapter.default_mode.value
        self.mode = require_mode(self.type, adapter.modes, named).value
        return self


class PipelineOptions(BaseModel):
    """Runtime options for pipeline execution."""

    threads: int = Field(default=4, ge=1, le=64)
    memory_limit: str = Field(default="2GB", pattern=r"^\d+[KMGT]B$")
    compute_counts: bool = True
    sample_data: bool = True
    sample_rows: int = Field(default=50, ge=1, le=100000)
    compute_summary: bool = False

    # New options for incremental and evolution
    full_refresh: bool = False
    schema_evolution: Literal["ignore", "fail", "evolve"] = "ignore"
    ignore_watermark: bool = False


class PipelineConfig(BaseModel):
    """Complete pipeline configuration."""

    source: SourceConfig
    target: TargetConfig
    options: dict[str, Any] = Field(default_factory=dict)

    def get_options(self, overrides: Optional[dict[str, Any]] = None) -> PipelineOptions:
        """
        Get pipeline options with overrides applied.

        Args:
            overrides: Optional dictionary of option overrides

        Returns:
            Validated PipelineOptions instance
        """
        opts = {**self.options}
        if overrides:
            opts.update(overrides)
        return PipelineOptions(**opts)
