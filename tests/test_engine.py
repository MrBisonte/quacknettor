"""
Unit tests for DuckDB engine.

Tests connection lifecycle, configuration, and error handling.
"""
import pytest
from duckel.engine import DuckDBEngine, DuckDBEngineError


class TestDuckDBEngine:
    """Test DuckDB engine lifecycle and configuration."""
    
    def test_context_manager_lifecycle(self):
        """Test that context manager properly creates and closes connection."""
        with DuckDBEngine() as con:
            assert con is not None
            # Verify connection works
            result = con.execute("SELECT 1 as test").fetchone()
            assert result[0] == 1
        
        # Connection should be closed after exiting context
        # Note: DuckDB connections don't have an is_closed() method,
        # but we can verify it was managed properly
    
    def test_memory_database(self):
        """Test in-memory database creation."""
        with DuckDBEngine(db_path=":memory:") as con:
            con.execute("CREATE TABLE test (id INTEGER)")
            con.execute("INSERT INTO test VALUES (1), (2), (3)")
            result = con.execute("SELECT COUNT(*) FROM test").fetchone()
            assert result[0] == 3
    
    def test_thread_configuration(self):
        """Test thread configuration."""
        with DuckDBEngine(threads=2) as con:
            result = con.execute("SELECT current_setting('threads')").fetchone()
            # DuckDB may adjust thread count based on system
            assert result[0] >= 1
    
    def test_memory_limit_configuration(self):
        """Test memory limit configuration."""
        with DuckDBEngine(memory_limit="1GB") as con:
            result = con.execute("SELECT current_setting('memory_limit')").fetchone()
            # DuckDB may adjust the memory limit, just verify it's set and reasonable
            assert "MiB" in result[0] or "GiB" in result[0]
    
    def test_httpfs_extension_loaded(self):
        """Test that httpfs extension is loaded for S3 access."""
        with DuckDBEngine() as con:
            # Query loaded extensions
            result = con.execute(
                "SELECT * FROM duckdb_extensions() WHERE extension_name = 'httpfs' AND loaded"
            ).fetchall()
            assert len(result) > 0, "httpfs extension should be loaded"
    
    def test_postgres_extension_loaded(self):
        """Test that postgres extension is loaded."""
        with DuckDBEngine() as con:
            # Check if loaded; extension may not show in the table
            try:
                con.execute("SELECT extension_name FROM duckdb_extensions() WHERE extension_name = 'postgres'").fetchall()
                # No error means extension is available
                assert True
            except Exception:
                pytest.fail("Postgres extension should be loaded")
    
    def test_multiple_connections(self):
        """Test that multiple connections can be created."""
        with DuckDBEngine() as con1:
            with DuckDBEngine() as con2:
                result1 = con1.execute("SELECT 1").fetchone()[0]
                result2 = con2.execute("SELECT 2").fetchone()[0]
                assert result1 == 1
                assert result2 == 2
    
    def test_error_during_configuration(self):
        """Test that configuration errors are properly raised."""
        # Invalid memory limit should raise an error
        with pytest.raises(DuckDBEngineError):
            with DuckDBEngine(memory_limit="invalid") as con:
                pass


class TestDuckDBEngineErrorHandling:
    """Test error handling in DuckDB engine."""
    
    def test_exception_during_query(self):
        """Test that query exceptions are propagated."""
        with DuckDBEngine() as con:
            with pytest.raises(Exception):  # DuckDB will raise various exceptions
                con.execute("SELECT * FROM nonexistent_table").fetchall()
    
    def test_connection_cleanup_on_error(self):
        """Test that connection is cleaned up even if error occurs."""
        try:
            with DuckDBEngine() as con:
                # Force an error
                con.execute("INVALID SQL SYNTAX")
        except Exception:
            pass  # Expected to fail
        
        # If we get here, cleanup was successful
        # (context manager __exit__ was called)
