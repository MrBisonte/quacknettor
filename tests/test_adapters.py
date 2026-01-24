"""
Unit tests for adapter classes.

Tests SQL injection protection, input validation, and SQL generation.
"""
import pytest
from duckel.adapters import (
    ParquetSourceAdapter,
    CSVSourceAdapter,
    PostgresSourceAdapter,
    SnowflakeSourceAdapter,
    ParquetTargetAdapter,
    CSVTargetAdapter,
    PostgresTargetAdapter,
    create_source_adapter,
    create_target_adapter,
    AdapterError,
    Adapter,
)


class TestAdapterBase:
    """Test base adapter functionality."""
    
    def test_sanitize_identifier_valid(self):
        """Test that valid identifiers pass sanitization."""
        valid_identifiers = [
            "table_name",
            "schema.table",
            "db.schema.table",
            "Table123",
            "my_table_2",
        ]
        
        for identifier in valid_identifiers:
            result = Adapter._sanitize_identifier(identifier)
            assert result == identifier
    
    def test_sanitize_identifier_invalid(self):
        """Test that invalid identifiers are rejected."""
        invalid_identifiers = [
            ("table; DROP TABLE users;--", "Invalid SQL identifier"),
            ("table'--", "Invalid SQL identifier"),
            ("table/*comment*/", "Invalid SQL identifier"),
            ("table name", "Invalid SQL identifier"),  # space
            ("table-name", "Invalid SQL identifier"),  # hyphen
            ("table@name", "Invalid SQL identifier"),  # special char
            ("", "Identifier cannot be empty"),  # empty - different message
        ]
        
        for identifier, expected_msg in invalid_identifiers:
            with pytest.raises(ValueError, match=expected_msg):
                Adapter._sanitize_identifier(identifier)


class TestParquetSourceAdapter:
    """Test Parquet source adapter."""
    
    def test_requires_path(self):
        """Test that Parquet source requires path."""
        with pytest.raises(ValueError, match="requires 'path'"):
            ParquetSourceAdapter({"type": "parquet"})
    
    def test_valid_local_path(self):
        """Test local Parquet path."""
        adapter = ParquetSourceAdapter({
            "type": "parquet",
            "path": "./data/file.parquet"
        })
        sql = adapter.get_relation_sql()
        assert sql == "read_parquet('./data/file.parquet')"
    
    def test_valid_s3_path(self):
        """Test S3 Parquet path."""
        adapter = ParquetSourceAdapter({
            "type": "parquet",
            "path": "s3://bucket/path/file.parquet"
        })
        sql = adapter.get_relation_sql()
        assert sql == "read_parquet('s3://bucket/path/file.parquet')"


class TestCSVSourceAdapter:
    """Test CSV source adapter."""
    
    def test_requires_path(self):
        """Test that CSV source requires path."""
        with pytest.raises(ValueError, match="requires 'path'"):
            CSVSourceAdapter({"type": "csv"})
    
    def test_valid_csv_path(self):
        """Test CSV path."""
        adapter = CSVSourceAdapter({
            "type": "csv",
            "path": "./data/file.csv"
        })
        sql = adapter.get_relation_sql()
        assert sql == "read_csv_auto('./data/file.csv')"


class TestPostgresSourceAdapter:
    """Test Postgres source adapter."""
    
    def test_requires_conn(self):
        """Test that Postgres source requires connection string."""
        with pytest.raises(ValueError, match="requires 'conn'"):
            PostgresSourceAdapter({"type": "postgres"})
    
    def test_sql_injection_in_object(self):
        """Test that SQL injection in object name is blocked."""
        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            adapter = PostgresSourceAdapter({
                "type": "postgres",
                "conn": "test",
                "object": "table; DROP TABLE users;--"
            })
            adapter.get_relation_sql()
    
    def test_sql_injection_in_name(self):
        """Test that SQL injection in name is blocked."""
        # Validation happens in Pydantic models, not in adapter __init__
        from pydantic import ValidationError
        from duckel.models import SourceConfig
        
        with pytest.raises(ValidationError, match="Invalid SQL identifier"):
            SourceConfig(
                type="postgres",
                conn="test",
                name="pg_source_attachment; DROP DATABASE;--",
                object="users"
            )
    
    def test_valid_object_simple(self):
        """Test simple table name."""
        adapter = PostgresSourceAdapter({
            "type": "postgres",
            "conn": "test",
            "object": "users"
        })
        sql = adapter.get_relation_sql()
        assert sql == "pg_source_attachment.users"
    
    def test_valid_object_qualified(self):
        """Test qualified table name."""
        adapter = PostgresSourceAdapter({
            "type": "postgres",
            "conn": "test",
            "object": "public.users"
        })
        sql = adapter.get_relation_sql()
        assert sql == "pg_source_attachment.public.users"
    
    def test_valid_query(self):
        """Test custom query."""
        adapter = PostgresSourceAdapter({
            "type": "postgres",
            "conn": "test",
            "query": "SELECT * FROM users WHERE active = true"
        })
        sql = adapter.get_relation_sql()
        assert "SELECT * FROM users WHERE active = true" in sql


class TestParquetTargetAdapter:
    """Test Parquet target adapter."""
    
    def test_requires_path(self):
        """Test that Parquet target requires path."""
        with pytest.raises(ValueError, match="requires 'path'"):
            ParquetTargetAdapter({"type": "parquet"})
    
    def test_write_sql_with_compression(self):
        """Test write SQL generation with compression."""
        adapter = ParquetTargetAdapter({
            "type": "parquet",
            "path": "./output.parquet",
            "compression": "zstd"
        })
        sql = adapter.build_write_sql("source_data")
        
        assert "COPY" in sql
        assert "source_data" in sql
        assert "./output.parquet" in sql
        assert "COMPRESSION zstd" in sql


class TestPostgresTargetAdapter:
    """Test Postgres target adapter."""
    
    def test_requires_conn_and_table(self):
        """Test that Postgres target requires connection for init, and table for write."""
        with pytest.raises(ValueError, match="requires 'conn'"):
            PostgresTargetAdapter({"type": "postgres", "table": "users"})
        
        # table is optional for init
        adapter = PostgresTargetAdapter({"type": "postgres", "conn": "test"})
        
        # but required for write
        with pytest.raises(ValueError, match="requires 'table'"):
            adapter.build_write_sql("source_data")
    
    def test_sql_injection_in_table(self):
        """Test that SQL injection in table name is blocked."""
        # Validation happens in Pydantic models
        from pydantic import ValidationError
        from duckel.models import TargetConfig
        
        with pytest.raises(ValidationError, match="Invalid SQL identifier"):
            TargetConfig(
                type="postgres",
                conn="test",
                table="users; DROP TABLE users;--"
            )
    
    def test_write_sql_overwrite_mode(self):
        """Test write SQL in overwrite mode."""
        adapter = PostgresTargetAdapter({
            "type": "postgres",
            "conn": "test",
            "table": "users",
            "mode": "overwrite"
        })
        sql = adapter.build_write_sql("source_data")
        
        assert "DROP TABLE IF EXISTS" in sql
        assert "CREATE TABLE" in sql
        assert "pg_target_attachment.users" in sql
    
    def test_write_sql_append_mode(self):
        """Test write SQL in append mode."""
        adapter = PostgresTargetAdapter({
            "type": "postgres",
            "conn": "test",
            "table": "users",
            "mode": "append"
        })
        sql = adapter.build_write_sql("source_data")
        
        assert "INSERT INTO" in sql
        assert "pg_target_attachment.users" in sql


class TestAdapterFactories:
    """Test adapter factory functions."""
    
    def test_create_source_adapter_parquet(self):
        """Test creating Parquet source adapter via factory."""
        adapter = create_source_adapter({
            "type": "parquet",
            "path": "./data.parquet"
        })
        assert isinstance(adapter, ParquetSourceAdapter)
    
    def test_create_source_adapter_postgres(self):
        """Test creating Postgres source adapter via factory."""
        adapter = create_source_adapter({
            "type": "postgres",
            "conn": "test",
            "object": "users"
        })
        assert isinstance(adapter, PostgresSourceAdapter)
    
    def test_create_source_adapter_unsupported(self):
        """Test that unsupported source type raises error."""
        with pytest.raises(ValueError, match="Unsupported source type"):
            create_source_adapter({"type": "unsupported"})
    
    def test_create_target_adapter_parquet(self):
        """Test creating Parquet target adapter via factory."""
        adapter = create_target_adapter({
            "type": "parquet",
            "path": "./output.parquet"
        })
        assert isinstance(adapter, ParquetTargetAdapter)
    
    def test_create_target_adapter_postgres(self):
        """Test creating Postgres target adapter via factory."""
        adapter = create_target_adapter({
            "type": "postgres",
            "conn": "test",
            "table": "users"
        })
        assert isinstance(adapter, PostgresTargetAdapter)
    
    def test_create_target_adapter_unsupported(self):
        """Test that unsupported target type raises error."""
        with pytest.raises(ValueError, match="Unsupported target type"):
            create_target_adapter({"type": "unsupported"})
