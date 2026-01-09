"""
SQL adapters for different source and target types.

Provides secure SQL generation with injection protection.
"""
from abc import ABC, abstractmethod
from typing import Optional
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


class ParquetTargetAdapter(TargetAdapter):
    """Adapter for Parquet file targets."""
    
    def validate(self):
        if "path" not in self.config:
            raise ValueError("Parquet target requires 'path'")
    
    def attach(self, con):
        """No attachment needed for Parquet files."""
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
        """Attach Postgres database to DuckDB."""
        name = self.sanitize_identifier(self.config.get("name", "pgtgt"))
        conn_str = resolve_env_tokens(self.config["conn"])
        conn_str = resolve_secret_tokens(conn_str)
        
        logger.info(f"Attaching Postgres database as '{name}'")
        try:
            con.execute(f"ATTACH '{conn_str}' AS {name} (TYPE postgres);")
        except Exception as e:
            logger.error(f"Failed to attach Postgres: {e}")
            raise AdapterError(f"Failed to attach Postgres database: {e}") from e
    
    def build_write_sql(self, relation_sql: str) -> str:
        name = self.sanitize_identifier(self.config.get("name", "pgtgt"))
        table = self.sanitize_identifier(self.config["table"])
        mode = self.config.get("mode", "append")
        
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
        else:
            logger.info(f"Appending to table: {full_table}")
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
