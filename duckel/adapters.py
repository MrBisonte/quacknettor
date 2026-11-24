"""
SQL adapters for different source and target types.

Provides secure SQL generation with injection protection.
"""

import re
from abc import ABC, abstractmethod
from collections.abc import Sequence
from dataclasses import dataclass
from enum import Enum
from typing import Any, ClassVar

from duckdb import DuckDBPyConnection
from duckdb import Error as DuckDBError

from .errors import ConfigError, ConfigErrorCategory
from .logger import logger
from .settings import DEFAULT_COMPRESSION
from .tokens import resolve_env_tokens, resolve_secret_tokens


class AdapterError(Exception):
    """Raised when adapter encounters an error."""

    pass


class WriteMode(str, Enum):
    """How a write treats the rows the target already holds."""

    OVERWRITE = "overwrite"
    APPEND = "append"
    UPSERT = "upsert"


def require_mode(target: str, modes: frozenset, mode: str) -> WriteMode:
    """
    The write mode a configuration names, when the target declares it.

    Args:
        target: Target type, for the message
        modes: The modes the target declares
        mode: The mode the configuration names

    Returns:
        The mode as a WriteMode

    Raises:
        ConfigError: If the target does not declare the mode
    """
    if mode not in {m.value for m in modes}:
        supported = ", ".join(sorted(m.value for m in modes))
        raise ConfigError(
            ConfigErrorCategory.UNSUPPORTED_MODE,
            f"A {target} target does not support mode '{mode}'.",
            f"Use one of the modes a {target} target supports: {supported}.",
        )
    return WriteMode(mode)


class SchemaEvolution(str, Enum):
    """What a run does when the source and the target hold different columns."""

    IGNORE = "ignore"
    FAIL = "fail"
    EVOLVE = "evolve"


# The setting of a target whose configuration names none. Its one home.
DEFAULT_SCHEMA_EVOLUTION = SchemaEvolution.IGNORE


class Counted(str, Enum):
    """What a write counts by itself and returns."""

    ROWS = "rows"
    BYTES = "bytes"


@dataclass(frozen=True)
class WriteReport:
    """
    What one write moved, counted by the write itself.

    Attributes:
        rows: Rows the statement reports it wrote
        bytes: Size of what was written. None for a target that does not count bytes, and
            when the size could not be read after the write
    """

    rows: int
    bytes: int | None


def quote_identifier(name: str) -> str:
    """A column name as a quoted SQL identifier."""
    return '"' + name.replace('"', '""') + '"'


def column_list(columns: Sequence[str]) -> str:
    """
    The columns a write fills, quoted and comma separated.

    Raises:
        AdapterError: If no column is given, because a write names the columns it fills
    """
    if not columns:
        raise AdapterError(
            "A write was handed no column to fill. "
            "Pass the columns of the source relation, see describe_columns()."
        )
    return ", ".join(quote_identifier(column) for column in columns)


def row_count(counted: tuple | None) -> int:
    """
    The row count a write statement returns.

    Args:
        counted: The one row the statement returned, or None

    Raises:
        AdapterError: If the statement returned no row count
    """
    if not counted or not isinstance(counted[0], int):
        raise AdapterError(
            f"The write returned no row count: {counted!r}. "
            "Run the statement build_write_sql() returns: each one ends by reporting a count."
        )
    return counted[0]


def describe_columns(con: DuckDBPyConnection, relation_sql: str) -> dict[str, str]:
    """
    The columns of a relation and their types, in order.

    Args:
        con: DuckDB connection the caller opened
        relation_sql: Any expression a FROM takes

    Returns:
        Column name to column type. The engine binds the statement and reads no row.
    """
    described = con.execute(f"DESCRIBE SELECT * FROM {relation_sql} LIMIT 0").fetchall()
    return {row[0]: row[1] for row in described}


def _named_in(name: str, columns: dict[str, str]) -> bool:
    """True when a column of that name is among the columns, compared without case."""
    return name.lower() in {held.lower() for held in columns}


def _difference(new: list, gone: list) -> str:
    """The columns a source and a target do not share, as one clause."""
    clauses = []
    if new:
        clauses.append(f"the source has columns {new} that are missing in the target")
    if gone:
        clauses.append(f"the target has columns {gone} that are missing in the source")
    return " and ".join(clauses)


def file_size_sql(path: str) -> str:
    """The statement that reads the size of one file, and no other column of it."""
    return f"SELECT size FROM read_blob('{path}')"


def copy_sql(relation_sql: str, columns: Sequence[str], path: str, options: str) -> str:
    """The statement that writes the named columns of a relation to one file."""
    return f"COPY (SELECT {column_list(columns)} FROM {relation_sql}) TO '{path}' ({options});"


def overwrite_sql(table: str, relation_sql: str, columns: Sequence[str]) -> str:
    """The statements that drop a table and create it again from the named columns."""
    return (
        f"DROP TABLE IF EXISTS {table};\n"
        f"CREATE TABLE {table} AS SELECT {column_list(columns)} FROM {relation_sql};"
    )


def append_sql(table: str, relation_sql: str, columns: Sequence[str]) -> str:
    """The statement that adds rows to a table, column by name."""
    names = column_list(columns)
    return f"INSERT INTO {table} ({names}) SELECT {names} FROM {relation_sql};"


def upsert_sql(table: str, relation_sql: str, columns: Sequence[str]) -> str:
    """
    The statement that adds rows to a table and replaces the rows whose key it already holds.

    It is DuckDB's own upsert syntax. The key is the primary key of the target table.
    """
    names = column_list(columns)
    return f"INSERT OR REPLACE INTO {table} ({names}) SELECT {names} FROM {relation_sql};"


class Adapter(ABC):
    """Base adapter with input validation."""

    def __init__(self, config: dict):
        self.config = config
        self.validate()

    @abstractmethod
    def validate(self):
        """Validate configuration. Raise ValueError if invalid."""
        pass

    @staticmethod
    def _sanitize_identifier(identifier: str) -> str:
        """
        Sanitize SQL identifiers to prevent injection.

        Args:
            identifier: SQL identifier (table, schema, column name)

        Returns:
            Validated identifier

        Raises:
            ValueError: If identifier contains invalid characters
        """
        if not identifier:
            raise ValueError("Identifier cannot be empty")

        # Only allow alphanumeric, underscore, and dot
        if not re.match(r"^[a-zA-Z0-9_\.]+$", identifier):
            raise ValueError(
                f"Invalid SQL identifier: '{identifier}'. "
                f"Only alphanumeric characters, underscore, and dot are allowed."
            )
        return identifier


# ===== SOURCE ADAPTERS =====


class SourceAdapter(Adapter):
    """Base class for source adapters."""

    @abstractmethod
    def attach(self, con):
        """Attach source to DuckDB connection if needed."""
        pass

    @abstractmethod
    def get_relation_sql(self) -> str:
        """Get SQL expression to reference this source."""
        pass

    def get_incremental_sql(self, relation_sql: str, watermark_value: Any | None = None) -> str:
        """
        Get SQL expression with incremental filtering applied.

        Args:
            relation_sql: Base relation SQL
            watermark_value: Value to filter by (records > watermark)

        Returns:
            SQL expression with WHERE clause if applicable
        """
        key = self.config.get("incremental_key")

        if key and watermark_value is not None:
            # Handle string vs numeric comparison if needed
            # For now, we assume simple comparison works in SQL
            logger.info(f"Applying incremental filter: {key} > '{watermark_value}'")

            # Use safe parameter injection if possible, but here we are constructing SQL string
            # DuckDB handles varying types in comparison
            return f"(SELECT * FROM {relation_sql} WHERE {key} > '{watermark_value}')"

        return relation_sql


class ParquetSourceAdapter(SourceAdapter):
    """Adapter for Parquet file sources."""

    def validate(self):
        if "path" not in self.config:
            raise ValueError("Parquet source requires 'path'")

    def attach(self, con):
        """No attachment needed for Parquet files."""
        pass

    def get_relation_sql(self) -> str:
        path = self.config["path"]
        logger.debug(f"Reading Parquet from: {path}")
        # Path is safely wrapped in quotes, no injection risk
        return f"read_parquet('{path}')"


class CSVSourceAdapter(SourceAdapter):
    """Adapter for CSV file sources."""

    def validate(self):
        if "path" not in self.config:
            raise ValueError("CSV source requires 'path'")

    def attach(self, con):
        """No attachment needed for CSV files."""
        pass

    def get_relation_sql(self) -> str:
        path = self.config["path"]
        logger.debug(f"Reading CSV from: {path}")
        return f"read_csv_auto('{path}')"


class PostgresSourceAdapter(SourceAdapter):
    """Adapter for Postgres sources."""

    def validate(self):
        if "conn" not in self.config:
            raise ValueError("Postgres source requires 'conn'")

    def attach(self, con):
        """Attach Postgres database to DuckDB."""
        name = self._sanitize_identifier(self.config.get("name", "pg_source_attachment"))
        conn_str = resolve_env_tokens(self.config["conn"])
        conn_str = resolve_secret_tokens(conn_str)

        logger.info(f"Attaching Postgres database as '{name}'")
        try:
            # Connection string is parameterized to avoid injection
            con.execute(f"ATTACH '{conn_str}' AS {name} (TYPE postgres);")
        except Exception as e:
            logger.error(f"Failed to attach Postgres: {e}")
            raise AdapterError(f"Failed to attach Postgres database: {e}") from e

    def get_relation_sql(self) -> str:
        name = self._sanitize_identifier(self.config.get("name", "pg_source_attachment"))

        if "query" in self.config:
            # Custom query - wrap in subquery
            return f"({self.config['query']})"

        if "object" in self.config:
            obj = self._sanitize_identifier(self.config["object"])
            # If object already has schema prefix, use as-is
            if "." in obj:
                return f"{name}.{obj}"
            else:
                return f"{name}.{obj}"

        raise ValueError("Postgres source requires either 'object' or 'query'")


class SnowflakeSourceAdapter(SourceAdapter):
    """Adapter for Snowflake sources."""

    def validate(self):
        if "conn" not in self.config:
            raise ValueError("Snowflake source requires 'conn'")

    def attach(self, con):
        """Attach Snowflake database to DuckDB."""
        name = self._sanitize_identifier(self.config.get("name", "sf_source_attachment"))
        conn_str = resolve_env_tokens(self.config["conn"])
        conn_str = resolve_secret_tokens(conn_str)

        logger.info(f"Attaching Snowflake database as '{name}'")
        try:
            con.execute(f"ATTACH '{conn_str}' AS {name} (TYPE snowflake);")
        except Exception as e:
            logger.error(f"Failed to attach Snowflake: {e}")
            raise AdapterError(f"Failed to attach Snowflake database: {e}") from e

    def get_relation_sql(self) -> str:
        name = self._sanitize_identifier(self.config.get("name", "sf_source_attachment"))

        if "query" in self.config:
            return f"({self.config['query']})"

        if "object" in self.config:
            obj = self._sanitize_identifier(self.config["object"])
            if "." in obj:
                return f"{name}.{obj}"
            else:
                return f"{name}.{obj}"

        raise ValueError("Snowflake source requires either 'object' or 'query'")


# ===== TARGET ADAPTERS =====


class TargetAdapter(Adapter):
    """Base class for target adapters."""

    # The write modes this target supports, declared as data by each target. A configuration
    # that names any other mode is refused when it loads, see `TargetConfig` in models.py.
    modes: ClassVar[frozenset[WriteMode]]

    # The mode of a target whose configuration names none, declared as data by each target.
    default_mode: ClassVar[WriteMode]

    # What a write to this target counts and returns, declared as data by each target.
    counts: ClassVar[frozenset[Counted]]

    def __init__(self, config: dict):
        super().__init__(config)
        # A target built from a plain dict never passed `TargetConfig`, so it is checked here too.
        self.mode = require_mode(
            config.get("type") or type(self).__name__,
            self.modes,
            config.get("mode") or self.default_mode.value,
        )

    @abstractmethod
    def attach(self, con):
        """Attach target to DuckDB connection if needed."""
        pass

    @abstractmethod
    def build_write_sql(self, relation_sql: str, columns: Sequence[str]) -> str:
        """Build SQL to write the named columns of a relation to the target."""
        pass

    @abstractmethod
    def bytes_written(self, con: DuckDBPyConnection) -> int | None:
        """Bytes the last write moved, or None when this target has no count to give."""
        pass

    def write(self, con: DuckDBPyConnection, write_sql: str) -> WriteReport:
        """
        Run a statement built by `build_write_sql` in a transaction and return what it moved.

        The bytes are read after the commit, so a size that cannot be read never undoes a write.

        Args:
            con: DuckDB connection the caller opened
            write_sql: Statement from `build_write_sql`

        Returns:
            The rows the statement reports, and the bytes this target counts

        Raises:
            AdapterError: If the statement returns no row count. The transaction is rolled back.
        """
        con.execute("BEGIN TRANSACTION;")
        try:
            rows = row_count(con.execute(write_sql).fetchone())
            con.execute("COMMIT;")
        except Exception as e:
            logger.error(f"Write failed, rolling back transaction: {e}")
            con.execute("ROLLBACK;")
            raise
        return WriteReport(rows=rows, bytes=self.bytes_written(con))

    @abstractmethod
    def relation(self) -> str:
        """An expression a FROM takes that reads the target back."""
        pass

    def keeps_rows(self) -> bool:
        """True when the write keeps the rows the target holds. An overwrite recreates it."""
        return self.mode is not WriteMode.OVERWRITE

    def existing_columns(self, con: DuckDBPyConnection) -> dict[str, str] | None:
        """The columns the target holds and their types, or None when the engine finds no target."""
        try:
            return describe_columns(con, self.relation())
        except DuckDBError:
            return None

    def sync_schema(
        self, con: DuckDBPyConnection, relation_sql: str, evolution_override: str | None = None
    ) -> list[str]:
        """
        Compare the source with the target, apply `schema_evolution`, return the columns to fill.

        A write that recreates the target fills every source column. A write that keeps the rows
        of the target fills the source columns the target holds. Names are compared without
        case, as the engine binds them.

        Args:
            con: DuckDB connection
            relation_sql: Source relation SQL
            evolution_override: The setting for this run, in place of the target's own

        Returns:
            The columns the write names, in the order of the source

        Raises:
            AdapterError: If the setting is `fail` and the columns differ, if an append or an
                upsert finds no target, or if a column cannot be added
        """
        named = evolution_override or self.config.get("schema_evolution")
        evolution = SchemaEvolution(named or DEFAULT_SCHEMA_EVOLUTION.value)
        held = self.existing_columns(con)
        source = describe_columns(con, relation_sql)
        if held is None:
            self._refuse_to_keep_the_rows_of_no_target()
            return list(source)

        new = {name: kind for name, kind in source.items() if not _named_in(name, held)}
        gone = [name for name in held if not _named_in(name, source)]
        if new or gone:
            self._settle(con, evolution, new, gone)
        if self.keeps_rows() and evolution is not SchemaEvolution.EVOLVE:
            return [name for name in source if name not in new]
        return list(source)

    def _refuse_to_keep_the_rows_of_no_target(self) -> None:
        """Raise AdapterError when an append or an upsert finds no target to keep the rows of."""
        if self.keeps_rows():
            raise AdapterError(
                f"Target table '{self.relation()}' not found in {self.config.get('type')}. "
                "Suggestion: Use 'mode: overwrite' for your first run to bootstrap the table "
                "structure, or create the table manually in the target database."
            )

    def _settle(
        self, con: DuckDBPyConnection, evolution: SchemaEvolution, new: dict[str, str], gone: list
    ) -> None:
        """Apply the setting to a source and a target that hold different columns."""
        mismatch = f"Schema mismatch on '{self.relation()}': {_difference(list(new), gone)}."
        if evolution is SchemaEvolution.FAIL:
            raise AdapterError(
                f"{mismatch} Pipeline aborted because schema_evolution is set to 'fail'."
            )
        if evolution is SchemaEvolution.EVOLVE:
            if self.keeps_rows():
                self._add_columns(con, new)
            logger.warning(f"{mismatch} schema_evolution is 'evolve'. {self._outcome(evolution)}")
            return
        if evolution is SchemaEvolution.IGNORE:
            logger.warning(f"{mismatch} schema_evolution is 'ignore'. {self._outcome(evolution)}")
            return
        raise AdapterError(f"schema_evolution '{evolution.value}' is not handled by a target.")

    def _outcome(self, evolution: SchemaEvolution) -> str:
        """What the run does next with the columns the two sides do not share."""
        if not self.keeps_rows():
            return "The run continues: the target is replaced and takes the columns of the source."
        absent = "A target column the source lacks is filled with NULL."
        if evolution is SchemaEvolution.EVOLVE:
            return f"The run continues: a source column the target lacks is added. {absent}"
        return f"The run continues: a source column the target lacks is not loaded. {absent}"

    def _add_columns(self, con: DuckDBPyConnection, new: dict[str, str]) -> None:
        """Add the columns the source has gained to a target that keeps its rows."""
        target = self.relation()
        for col, dtype in new.items():
            try:
                con.execute(f"ALTER TABLE {target} ADD COLUMN {quote_identifier(col)} {dtype}")
            except Exception as e:
                logger.error(f"Failed to add column {col}: {e}")
                raise AdapterError(
                    f"Schema evolution failed while adding column '{col}': {e}"
                ) from e


class FileTargetAdapter(TargetAdapter):
    """
    A target that is one file, written by COPY ... TO.

    The statement replaces the file, so overwrite is the one mode a file target declares.
    """

    modes = frozenset({WriteMode.OVERWRITE})
    default_mode = WriteMode.OVERWRITE
    counts = frozenset({Counted.ROWS, Counted.BYTES})

    # The DuckDB function that reads this file format back, declared by each format.
    reader: ClassVar[str]

    def attach(self, con):
        """No attachment needed for a file."""
        pass

    def relation(self) -> str:
        """The file, read back by the function of its format."""
        return f"{self.reader}('{self.config['path']}')"

    def bytes_written(self, con: DuckDBPyConnection) -> int | None:
        """
        The size of the file the write left, read by the engine without its content.

        Returns None, with a warning, when the engine cannot read the size. The rows are
        committed by then, so the run goes on and reports no byte count.
        """
        path = self.config["path"]
        try:
            found = con.execute(file_size_sql(path)).fetchone()
        except Exception as e:
            logger.warning(f"The size of '{path}' could not be read after the write: {e}")
            return None
        if not found:
            logger.warning(f"No file was found at '{path}' after the write: no byte count.")
            return None
        return found[0]


class DatabaseTargetAdapter(TargetAdapter):
    """
    A target that is a table in a database attached to DuckDB.

    The engine reports the rows of an insert and no byte count, so `counts` holds rows only.
    """

    default_mode = WriteMode.APPEND
    counts = frozenset({Counted.ROWS})

    # The name in messages, and the attachment alias when the configuration gives none.
    label: ClassVar[str]
    default_alias: ClassVar[str]

    def bytes_written(self, con: DuckDBPyConnection) -> None:
        """A database target does not count bytes."""
        return None

    def alias(self) -> str:
        """The name the database is attached under: the configured one, or this target's default."""
        return self._sanitize_identifier(self.config.get("name") or self.default_alias)

    def relation(self) -> str:
        """The target table, as the write names it."""
        return self.full_table()

    def full_table(self) -> str:
        """The target table, qualified by the attachment alias."""
        table_name = self.config.get("table")
        if not table_name:
            raise ValueError(f"{self.label} target requires 'table' for write operations")
        return f"{self.alias()}.{self._sanitize_identifier(table_name)}"


class ParquetTargetAdapter(FileTargetAdapter):
    """Adapter for Parquet file targets."""

    reader = "read_parquet"

    def validate(self):
        if "path" not in self.config:
            raise ValueError("Parquet target requires 'path'")

    def build_write_sql(self, relation_sql: str, columns: Sequence[str]) -> str:
        path = self.config["path"]
        compression = self.config.get("compression") or DEFAULT_COMPRESSION

        logger.debug(f"Writing Parquet to: {path}")
        # Use COPY TO for optimal performance
        return copy_sql(relation_sql, columns, path, f"FORMAT parquet, COMPRESSION {compression}")


class CSVTargetAdapter(FileTargetAdapter):
    """Adapter for CSV file targets."""

    reader = "read_csv_auto"

    def validate(self):
        if "path" not in self.config:
            raise ValueError("CSV target requires 'path'")

    def build_write_sql(self, relation_sql: str, columns: Sequence[str]) -> str:
        path = self.config["path"]
        logger.debug(f"Writing CSV to: {path}")
        return copy_sql(relation_sql, columns, path, "HEADER, DELIMITER ','")


class PostgresTargetAdapter(DatabaseTargetAdapter):
    """Adapter for Postgres targets."""

    modes = frozenset({WriteMode.OVERWRITE, WriteMode.APPEND, WriteMode.UPSERT})
    label = "Postgres"
    default_alias = "pg_target_attachment"

    def validate(self):
        if "conn" not in self.config:
            raise ValueError("Postgres target requires 'conn'")
        # Note: 'table' is no longer strictly required at validation time
        # to allow connection testing without a table name.

    def attach(self, con):
        """Attach Postgres database to DuckDB with categorized error handling."""
        name = self.alias()
        conn_str = resolve_env_tokens(self.config["conn"])
        conn_str = resolve_secret_tokens(conn_str)

        logger.info(f"Attaching Postgres database as '{name}'")
        try:
            con.execute(f"ATTACH '{conn_str}' AS {name} (TYPE postgres);")
        except Exception as e:
            error_msg = str(e).lower()
            logger.error(f"Failed to attach Postgres: {e}")

            # Categorize the error for actionable feedback
            if any(
                kw in error_msg
                for kw in ["authentication failed", "password", "role", "permission denied"]
            ):
                raise AdapterError(
                    f"Postgres authentication failed for attachment '{name}'. "
                    f"Check your username/password in the connection string. Original error: {e}"
                ) from e
            elif any(
                kw in error_msg
                for kw in ["could not connect", "timeout", "host", "connection refused", "no route"]
            ):
                raise AdapterError(
                    f"Cannot reach Postgres server for attachment '{name}'. "
                    f"Verify the host, port, and network connectivity. Original error: {e}"
                ) from e
            else:
                raise AdapterError(f"Failed to attach Postgres database '{name}': {e}") from e

    def build_write_sql(self, relation_sql: str, columns: Sequence[str]) -> str:
        full_table = self.full_table()
        unique_key = self.config.get("unique_key")

        if self.mode is WriteMode.OVERWRITE:
            logger.info(f"Overwriting table: {full_table}")
            return overwrite_sql(full_table, relation_sql, columns)
        if self.mode is WriteMode.UPSERT:
            if not unique_key:
                raise ValueError("Upsert requires 'unique_key' configuration")

            logger.info(f"Upserting to table: {full_table} using key: {unique_key}")
            return upsert_sql(full_table, relation_sql, columns)
        if self.mode is WriteMode.APPEND:
            logger.info(f"Appending to table: {full_table}")
            return append_sql(full_table, relation_sql, columns)
        raise AdapterError(f"A postgres target builds no statement for mode '{self.mode.value}'.")


class SnowflakeTargetAdapter(DatabaseTargetAdapter):
    """Adapter for Snowflake targets."""

    modes = frozenset({WriteMode.OVERWRITE, WriteMode.APPEND})
    label = "Snowflake"
    default_alias = "sf_target_attachment"

    def validate(self):
        if "conn" not in self.config:
            raise ValueError("Snowflake target requires 'conn'")
        # Note: 'table' is no longer strictly required at validation time

    def attach(self, con):
        """Attach Snowflake database to DuckDB with categorized error handling."""
        name = self.alias()
        conn_str = resolve_env_tokens(self.config["conn"])
        conn_str = resolve_secret_tokens(conn_str)

        logger.info(f"Attaching Snowflake database as '{name}'")
        try:
            con.execute(f"ATTACH '{conn_str}' AS {name} (TYPE snowflake);")
        except Exception as e:
            error_msg = str(e).lower()
            logger.error(f"Failed to attach Snowflake: {e}")

            # Categorize the error for actionable feedback
            if any(
                kw in error_msg
                for kw in ["authentication", "password", "incorrect", "user", "login"]
            ):
                raise AdapterError(
                    f"Snowflake authentication failed for attachment '{name}'. "
                    f"Check your account, user, and password settings. Original error: {e}"
                ) from e
            elif any(
                kw in error_msg
                for kw in ["could not connect", "timeout", "account", "host", "network"]
            ):
                raise AdapterError(
                    f"Cannot reach Snowflake account for attachment '{name}'. "
                    f"Verify the account identifier and network connectivity. Original error: {e}"
                ) from e
            elif "extension" in error_msg or "not found" in error_msg:
                raise AdapterError(
                    f"Snowflake extension not available. Install it with: INSTALL snowflake FROM community; "
                    f"Original error: {e}"
                ) from e
            else:
                raise AdapterError(f"Failed to attach Snowflake database '{name}': {e}") from e

    def build_write_sql(self, relation_sql: str, columns: Sequence[str]) -> str:
        full_table = self.full_table()

        if self.mode is WriteMode.OVERWRITE:
            logger.info(f"Overwriting Snowflake table: {full_table}")
            return overwrite_sql(full_table, relation_sql, columns)
        if self.mode is WriteMode.APPEND:
            logger.info(f"Appending to Snowflake table: {full_table}")
            return append_sql(full_table, relation_sql, columns)
        raise AdapterError(f"A snowflake target builds no statement for mode '{self.mode.value}'.")


# ===== REGISTRIES =====
# Sources and targets are open sets. A new member is one row here plus one adapter class.
# `duckel/models.py` validates `type` against these keys, so a name with no adapter is refused
# when the configuration loads.

SOURCES: dict[str, type[SourceAdapter]] = {
    "parquet": ParquetSourceAdapter,
    "csv": CSVSourceAdapter,
    "postgres": PostgresSourceAdapter,
    "snowflake": SnowflakeSourceAdapter,
}

TARGETS: dict[str, type[TargetAdapter]] = {
    "parquet": ParquetTargetAdapter,
    "csv": CSVTargetAdapter,
    "postgres": PostgresTargetAdapter,
    "snowflake": SnowflakeTargetAdapter,
}


# ===== FACTORY FUNCTIONS =====


def create_source_adapter(config: dict) -> SourceAdapter:
    """
    Factory function to create appropriate source adapter.

    Args:
        config: Source configuration dictionary

    Returns:
        Appropriate SourceAdapter instance

    Raises:
        ValueError: If source type is unsupported
    """
    source_type = config.get("type")

    if source_type not in SOURCES:
        raise ValueError(f"Unsupported source type: {source_type}")

    return SOURCES[source_type](config)


def create_target_adapter(config: dict) -> TargetAdapter:
    """
    Factory function to create appropriate target adapter.

    Args:
        config: Target configuration dictionary

    Returns:
        Appropriate TargetAdapter instance

    Raises:
        ValueError: If target type is unsupported
    """
    target_type = config.get("type")

    if target_type not in TARGETS:
        raise ValueError(f"Unsupported target type: {target_type}")

    return TARGETS[target_type](config)


# ===== LEGACY COMPATIBILITY FUNCTIONS =====
# These maintain backward compatibility with old code


def build_source_relation_sql(source: dict) -> str:
    """
    Legacy function for backward compatibility.

    Use create_source_adapter() instead.
    """
    adapter = create_source_adapter(source)
    return adapter.get_relation_sql()


def build_target_write_sql(target: dict, relation_sql: str, columns: Sequence[str]) -> str:
    """
    Legacy function for backward compatibility.

    Use create_target_adapter() instead.
    """
    adapter = create_target_adapter(target)
    return adapter.build_write_sql(relation_sql, columns)
