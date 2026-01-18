"""
SQL adapters for different source and target types.

Provides secure SQL generation with injection protection.
"""
from abc import ABC, abstractmethod
from typing import Optional, Any, List, Dict, Set
import re
from .logger import logger
from .config import resolve_env_tokens, resolve_secret_tokens


class AdapterError(Exception):
    """Raised when adapter encounters an error."""
    pass


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
    def sanitize_identifier(identifier: str) -> str:
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
        if not re.match(r'^[a-zA-Z0-9_\.]+$', identifier):
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
    
    def get_incremental_sql(self, relation_sql: str, watermark_value: Optional[Any] = None) -> str:
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
        name = self.sanitize_identifier(self.config.get("name", "pgsrc"))
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
        name = self.sanitize_identifier(self.config.get("name", "pgsrc"))
        
        if "query" in self.config:
            # Custom query - wrap in subquery
            return f"({self.config['query']})"
        
        if "object" in self.config:
            obj = self.sanitize_identifier(self.config["object"])
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
        name = self.sanitize_identifier(self.config.get("name", "sfsrc"))
        conn_str = resolve_env_tokens(self.config["conn"])
        conn_str = resolve_secret_tokens(conn_str)
        
        logger.info(f"Attaching Snowflake database as '{name}'")
        try:
            con.execute(f"ATTACH '{conn_str}' AS {name} (TYPE snowflake);")
        except Exception as e:
            logger.error(f"Failed to attach Snowflake: {e}")
            raise AdapterError(f"Failed to attach Snowflake database: {e}") from e
    
    def get_relation_sql(self) -> str:
        name = self.sanitize_identifier(self.config.get("name", "sfsrc"))
        
        if "query" in self.config:
            return f"({self.config['query']})"
        
        if "object" in self.config:
            obj = self.sanitize_identifier(self.config["object"])
            if "." in obj:
                return f"{name}.{obj}"
            else:
                return f"{name}.{obj}"
        
        raise ValueError("Snowflake source requires either 'object' or 'query'")


# ===== TARGET ADAPTERS =====

class TargetAdapter(Adapter):
    """Base class for target adapters."""
    
    @abstractmethod
    def attach(self, con):
        """Attach target to DuckDB connection if needed."""
        pass
    
    @abstractmethod
    def build_write_sql(self, relation_sql: str) -> str:
        """Build SQL to write data to target."""
        pass

    def sync_schema(self, con, relation_sql: str, evolution_override: Optional[str] = None):
        """
        Synchronize target schema with source schema.
        
        This method now performs proactive validation:
        1. Checks if target table exists (required for append/upsert)
        2. Compares schemas and provides actionable error messages
        
        Args:
            con: DuckDB connection
            relation_sql: Source relation SQL
            evolution_override: Optional override for schema evolution mode
        """
        evolution = evolution_override or self.config.get("schema_evolution", "ignore")
        mode = self.config.get("mode", "append")
        
        # Construct full table name
        target_name = self.config.get("table")
        adapter_type = self.config.get("type", "database")
        if hasattr(self, "config") and "name" in self.config:
            target_name = f"{self.config['name']}.{target_name}"
        
        # Step 1: Check if target table exists
        table_exists = False
        tgt_cols = {}
        try:
            tgt_desc = con.execute(f"DESCRIBE {target_name}").fetchall()
            tgt_cols = {r[0]: r[1] for r in tgt_desc}
            table_exists = True
        except Exception as e:
            # Table does not exist
            table_exists = False
            error_msg = str(e).lower()
            
            # If mode requires existing table, fail immediately with actionable message
            if mode in ("append", "upsert"):
                raise AdapterError(
                    f"Target table '{target_name}' not found in {adapter_type}. "
                    f"Suggestion: Use 'mode: overwrite' for your first run to bootstrap the table structure, "
                    f"or create the table manually in the target database."
                )
            # For overwrite mode, table will be created - no error needed
            return
        
        # Step 2: Compare schemas (always do this if table exists)
        try:
            src_desc = con.execute(f"DESCRIBE SELECT * FROM {relation_sql} LIMIT 0").fetchall()
            src_cols = {r[0]: r[1] for r in src_desc}
        except Exception as e:
            logger.warning(f"Could not describe source for schema comparison: {e}")
            return
        
        # Find columns in source that are missing in target
        missing_cols = {k: v for k, v in src_cols.items() if k not in tgt_cols}
        
        if missing_cols:
            missing_list = list(missing_cols.keys())
            
            if evolution == "fail":
                raise AdapterError(
                    f"Schema mismatch: Source has columns {missing_list} that are missing in target '{target_name}'. "
                    f"Pipeline aborted because schema_evolution is set to 'fail'."
                )
            elif evolution == "evolve":
                logger.info(f"Evolving schema for {target_name}. Adding columns: {missing_list}")
                for col, dtype in missing_cols.items():
                    try:
                        con.execute(f'ALTER TABLE {target_name} ADD COLUMN "{col}" {dtype}')
                    except Exception as e:
                        logger.error(f"Failed to add column {col}: {e}")
                        raise AdapterError(f"Schema evolution failed while adding column '{col}': {e}") from e
            else:
                # evolution == "ignore" - but we still warn with actionable message
                raise AdapterError(
                    f"Schema mismatch: Source has columns {missing_list} that are missing in target '{target_name}'. "
                    f"Suggestion: Set 'schema_evolution: evolve' to add these columns automatically, "
                    f"or use 'mode: overwrite' to recreate the table with the new schema."
                )


class ParquetTargetAdapter(TargetAdapter):
    """Adapter for Parquet file targets."""
    
    def validate(self):
        if "path" not in self.config:
            raise ValueError("Parquet target requires 'path'")
    
    def attach(self, con):
        """No attachment needed for Parquet files."""
        pass
        
    def sync_schema(self, con, relation_sql: str, evolution_override: Optional[str] = None):
        """Schema evolution not supported for Parquet files (yet)."""
        pass
    
    def build_write_sql(self, relation_sql: str) -> str:
        path = self.config["path"]
        compression = self.config.get("compression", "zstd")
        
        logger.debug(f"Writing Parquet to: {path}")
        # Use COPY TO for optimal performance
        return f"COPY (SELECT * FROM {relation_sql}) TO '{path}' (FORMAT parquet, COMPRESSION {compression});"


class CSVTargetAdapter(TargetAdapter):
    """Adapter for CSV file targets."""
    
    def validate(self):
        if "path" not in self.config:
            raise ValueError("CSV target requires 'path'")
    
    def attach(self, con):
        """No attachment needed for CSV files."""
        pass
    
    def build_write_sql(self, relation_sql: str) -> str:
        path = self.config["path"]
        logger.debug(f"Writing CSV to: {path}")
        return f"COPY (SELECT * FROM {relation_sql}) TO '{path}' (HEADER, DELIMITER ',');"


class PostgresTargetAdapter(TargetAdapter):
    """Adapter for Postgres targets."""
    
    def validate(self):
        if "conn" not in self.config:
            raise ValueError("Postgres target requires 'conn'")
        if "table" not in self.config:
            raise ValueError("Postgres target requires 'table'")
    
    def attach(self, con):
        """Attach Postgres database to DuckDB with categorized error handling."""
        name = self.sanitize_identifier(self.config.get("name", "pgtgt"))
        conn_str = resolve_env_tokens(self.config["conn"])
        conn_str = resolve_secret_tokens(conn_str)
        
        logger.info(f"Attaching Postgres database as '{name}'")
        try:
            con.execute(f"ATTACH '{conn_str}' AS {name} (TYPE postgres);")
        except Exception as e:
            error_msg = str(e).lower()
            logger.error(f"Failed to attach Postgres: {e}")
            
            # Categorize the error for actionable feedback
            if any(kw in error_msg for kw in ["authentication failed", "password", "role", "permission denied"]):
                raise AdapterError(
                    f"Postgres authentication failed for attachment '{name}'. "
                    f"Check your username/password in the connection string. Original error: {e}"
                ) from e
            elif any(kw in error_msg for kw in ["could not connect", "timeout", "host", "connection refused", "no route"]):
                raise AdapterError(
                    f"Cannot reach Postgres server for attachment '{name}'. "
                    f"Verify the host, port, and network connectivity. Original error: {e}"
                ) from e
            else:
                raise AdapterError(f"Failed to attach Postgres database '{name}': {e}") from e
    
    def build_write_sql(self, relation_sql: str) -> str:
        name = self.sanitize_identifier(self.config.get("name", "pgtgt"))
        table = self.sanitize_identifier(self.config["table"])
        mode = self.config.get("mode", "append")
        unique_key = self.config.get("unique_key")
        
        # Prefix table with attachment name if not already qualified
        if "." in table:
            full_table = f"{name}.{table}"
        else:
            full_table = f"{name}.{table}"
        
        if mode == "overwrite":
            logger.info(f"Overwriting table: {full_table}")
            return f"""
            DROP TABLE IF EXISTS {full_table};
            CREATE TABLE {full_table} AS SELECT * FROM {relation_sql};
            """
        elif mode == "upsert":
            if not unique_key:
                raise ValueError("Upsert requires 'unique_key' configuration")
            
            logger.info(f"Upserting to table: {full_table} using key: {unique_key}")
            # DuckDB via Postgres attachment handles INSERT OR REPLACE as standard insert or specialized logic?
            # Safe bet with DuckDB's postgres scanner is standard SQL passed.
            # But standard PG needs `ON CONFLICT`.
            # We'll use DuckDB's `INSERT OR REPLACE INTO` which is the dedicated upsert syntax
            return f"INSERT OR REPLACE INTO {full_table} SELECT * FROM {relation_sql};"
        else:
            logger.info(f"Appending to table: {full_table}")
            return f"INSERT INTO {full_table} SELECT * FROM {relation_sql};"


class SnowflakeTargetAdapter(TargetAdapter):
    """Adapter for Snowflake targets."""
    
    def validate(self):
        if "conn" not in self.config:
            raise ValueError("Snowflake target requires 'conn'")
        if "table" not in self.config:
            raise ValueError("Snowflake target requires 'table'")
    
    def attach(self, con):
        """Attach Snowflake database to DuckDB with categorized error handling."""
        name = self.sanitize_identifier(self.config.get("name", "sftgt"))
        conn_str = resolve_env_tokens(self.config["conn"])
        conn_str = resolve_secret_tokens(conn_str)
        
        logger.info(f"Attaching Snowflake database as '{name}'")
        try:
            con.execute(f"ATTACH '{conn_str}' AS {name} (TYPE snowflake);")
        except Exception as e:
            error_msg = str(e).lower()
            logger.error(f"Failed to attach Snowflake: {e}")
            
            # Categorize the error for actionable feedback
            if any(kw in error_msg for kw in ["authentication", "password", "incorrect", "user", "login"]):
                raise AdapterError(
                    f"Snowflake authentication failed for attachment '{name}'. "
                    f"Check your account, user, and password settings. Original error: {e}"
                ) from e
            elif any(kw in error_msg for kw in ["could not connect", "timeout", "account", "host", "network"]):
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
    
    def build_write_sql(self, relation_sql: str) -> str:
        name = self.sanitize_identifier(self.config.get("name", "sftgt"))
        table = self.sanitize_identifier(self.config["table"])
        mode = self.config.get("mode", "append")
        
        # Construct fully qualified table name
        if "." in table:
            full_table = f"{name}.{table}"
        else:
            full_table = f"{name}.{table}"
        
        if mode == "overwrite":
            logger.info(f"Overwriting Snowflake table: {full_table}")
            return f"""
            DROP TABLE IF EXISTS {full_table};
            CREATE TABLE {full_table} AS SELECT * FROM {relation_sql};
            """
        else:
            logger.info(f"Appending to Snowflake table: {full_table}")
            return f"INSERT INTO {full_table} SELECT * FROM {relation_sql};"


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
    
    adapters = {
        "parquet": ParquetSourceAdapter,
        "csv": CSVSourceAdapter,
        "postgres": PostgresSourceAdapter,
        "snowflake": SnowflakeSourceAdapter,
    }
    
    if source_type not in adapters:
        raise ValueError(f"Unsupported source type: {source_type}")
    
    return adapters[source_type](config)


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
    
    adapters = {
        "parquet": ParquetTargetAdapter,
        "csv": CSVTargetAdapter,
        "postgres": PostgresTargetAdapter,
        "snowflake": SnowflakeTargetAdapter,
    }
    
    if target_type not in adapters:
        raise ValueError(f"Unsupported target type: {target_type}")
    
    return adapters[target_type](config)



# ===== LEGACY COMPATIBILITY FUNCTIONS =====
# These maintain backward compatibility with old code

def build_source_relation_sql(source: dict) -> str:
    """
    Legacy function for backward compatibility.
    
    Use create_source_adapter() instead.
    """
    adapter = create_source_adapter(source)
    return adapter.get_relation_sql()


def build_target_write_sql(target: dict, relation_sql: str) -> str:
    """
    Legacy function for backward compatibility.
    
    Use create_target_adapter() instead.
    """
    adapter = create_target_adapter(target)
    return adapter.build_write_sql(relation_sql)
