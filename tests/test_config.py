"""
Unit tests for configuration loading and validation.

Tests Pydantic models and YAML config loading.
"""
import pytest
import tempfile
import os
from pathlib import Path
from duckel.config import load_config, resolve_env_tokens, resolve_secret_tokens
from duckel.models import SourceConfig, TargetConfig, PipelineConfig, PipelineOptions
from pydantic import ValidationError


class TestSourceConfig:
    """Test SourceConfig Pydantic model."""
    
    def test_valid_parquet_source(self):
        """Test valid Parquet source configuration."""
        config = SourceConfig(type="parquet", path="./data.parquet")
        assert config.type == "parquet"
        assert config.path == "./data.parquet"
    
    def test_parquet_source_requires_path(self):
        """Test that Parquet source requires path."""
        # Pydantic v2 doesn't raise ValidationError immediately for missing fields after validation
        # The validation only happens when used in adapters
        from duckel.adapters import ParquetSourceAdapter
        with pytest.raises(ValueError, match="requires 'path'"):
            ParquetSourceAdapter({"type": "parquet"})
    
    def test_valid_postgres_source(self):
        """Test valid Postgres source configuration."""
        config = SourceConfig(
            type="postgres",
            conn="host=localhost",
            object="public.users"
        )
        assert config.type == "postgres"
        assert config.conn == "host=localhost"
    
    def test_postgres_source_requires_conn(self):
        """Test that Postgres source requires connection string."""
        from duckel.adapters import PostgresSourceAdapter
        with pytest.raises(ValueError, match="requires 'conn'"):
            PostgresSourceAdapter({"type": "postgres", "object": "users"})
    
    def test_sql_injection_protection_in_object(self):
        """Test that SQL injection is blocked in object name."""
        with pytest.raises(ValidationError, match="Invalid SQL identifier"):
            SourceConfig(
                type="postgres",
                conn="test",
                object="users; DROP TABLE users;--"
            )
    
    def test_sql_injection_protection_in_name(self):
        """Test that SQL injection is blocked in name."""
        with pytest.raises(ValidationError, match="Invalid SQL identifier"):
            SourceConfig(
                type="postgres",
                conn="test",
                name="src; DROP DATABASE;--",
                object="users"
            )


class TestTargetConfig:
    """Test TargetConfig Pydantic model."""
    
    def test_valid_parquet_target(self):
        """Test valid Parquet target configuration."""
        config = TargetConfig(type="parquet", path="./output.parquet")
        assert config.type == "parquet"
        assert config.path == "./output.parquet"
        assert config.mode == "append"  # default
    
    def test_parquet_target_with_mode(self):
        """Test Parquet target with mode."""
        config = TargetConfig(
            type="parquet",
            path="./output.parquet",
            mode="overwrite"
        )
        assert config.mode == "overwrite"
    
    def test_postgres_target_relaxed_table_validation(self):
        """Test that Postgres target allows missing table during init (for connection testing)."""
        from duckel.adapters import PostgresTargetAdapter
        # This should NOT raise now
        adapter = PostgresTargetAdapter({"type": "postgres", "conn": "test"})
        
        # But it SHOULD raise when trying to write
        with pytest.raises(ValueError, match="requires 'table'"):
            adapter.build_write_sql("source_data")
    
    def test_invalid_mode(self):
        """Test that invalid mode is rejected."""
        with pytest.raises(ValidationError):
            TargetConfig(
                type="parquet",
                path="./output.parquet",
                mode="invalid_mode"
            )


class TestPipelineOptions:
    """Test PipelineOptions model."""
    
    def test_default_options(self):
        """Test default pipeline options."""
        opts = PipelineOptions()
        assert opts.threads == 4
        assert opts.memory_limit == "2GB"
        assert opts.compute_counts is True
        assert opts.sample_data is True
        assert opts.sample_rows == 50
    
    def test_thread_validation(self):
        """Test thread count validation."""
        # Valid
        opts = PipelineOptions(threads=8)
        assert opts.threads == 8
        
        # Too low
        with pytest.raises(ValidationError):
            PipelineOptions(threads=0)
        
        # Too high
        with pytest.raises(ValidationError):
            PipelineOptions(threads=100)
    
    def test_sample_rows_validation(self):
        """Test sample rows validation."""
        # Valid
        opts = PipelineOptions(sample_rows=100)
        assert opts.sample_rows == 100
        
        # Too low
        with pytest.raises(ValidationError):
            PipelineOptions(sample_rows=0)


class TestPipelineConfig:
    """Test PipelineConfig model."""
    
    def test_valid_pipeline(self):
        """Test valid pipeline configuration."""
        config = PipelineConfig(
            source={"type": "parquet", "path": "./input.parquet"},
            target={"type": "parquet", "path": "./output.parquet"}
        )
        assert config.source.type == "parquet"
        assert config.target.type == "parquet"
    
    def test_get_options_with_overrides(self):
        """Test getting options with overrides."""
        config = PipelineConfig(
            source={"type": "parquet", "path": "./input.parquet"},
            target={"type": "parquet", "path": "./output.parquet"},
            options={"threads": 4, "sample_rows": 100}
        )
        
        # Get options with override
        opts = config.get_options({"sample_rows": 200})
        assert opts.threads == 4
        assert opts.sample_rows == 200


class TestConfigLoading:
    """Test configuration file loading."""
    
    def test_load_valid_config(self, tmp_path):
        """Test loading valid YAML config."""
        config_file = tmp_path / "pipelines.yml"
        config_file.write_text("""
pipelines:
  test_pipeline:
    source:
      type: parquet
      path: ./input.parquet
    target:
      type: parquet
      path: ./output.parquet
    options:
      threads: 4
""")
        
        pipelines = load_config(str(config_file))
        assert "test_pipeline" in pipelines
        assert pipelines["test_pipeline"].source.type == "parquet"
    
    def test_load_config_file_not_found(self):
        """Test that missing config file raises error."""
        with pytest.raises(FileNotFoundError):
            load_config("nonexistent.yml")
    
    def test_load_config_missing_pipelines_key(self, tmp_path):
        """Test that config without pipelines key raises error."""
        config_file = tmp_path / "bad_config.yml"
        config_file.write_text("not_pipelines: {}")
        
        with pytest.raises(ValueError, match="Missing top-level 'pipelines' key"):
            load_config(str(config_file))
    
    def test_load_config_invalid_pipeline(self, tmp_path):
        """Test that invalid pipeline config raises error."""
        config_file = tmp_path / "invalid.yml"
        config_file.write_text("""
pipelines:
  bad_pipeline:
    source:
      type: invalid_type
      path: ./test.parquet
    target:
      type: parquet
      path: ./output.parquet
""")
        
        with pytest.raises(ValueError, match="Invalid configuration"):
            load_config(str(config_file))


class TestEnvironmentTokens:
    """Test environment variable resolution."""
    
    def test_resolve_env_tokens(self):
        """Test resolving __ENV: tokens."""
        os.environ["TEST_VAR"] = "test_value"
        
        result = resolve_env_tokens("prefix__ENV:TEST_VARsuffix")
        assert result == "prefixtest_valuesuffix"
        
        # Cleanup
        del os.environ["TEST_VAR"]
    
    def test_resolve_env_tokens_not_found(self):
        """Test that missing env var returns empty string."""
        result = resolve_env_tokens("__ENV:NONEXISTENT_VAR")
        assert result == ""
    
    def test_resolve_env_tokens_non_string(self):
        """Test that non-strings are returned as-is."""
        assert resolve_env_tokens(123) == 123
        assert resolve_env_tokens(None) is None


class TestSecretTokens:
    """Test secret resolution."""
    
    def test_resolve_secret_tokens(self):
        """Test resolving SECRET: tokens."""
        os.environ["TEST_SECRET"] = "secret_value"
        
        result = resolve_secret_tokens("SECRET:TEST_SECRET")
        assert result == "secret_value"
        
        # Cleanup
        del os.environ["TEST_SECRET"]
    
    def test_resolve_secret_tokens_not_found(self):
        """Test that missing secret returns empty string."""
        result = resolve_secret_tokens("SECRET:NONEXISTENT")
        assert result == ""
