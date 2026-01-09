"""
DuckDB engine management with proper lifecycle and error handling.
"""
import os
import duckdb
from typing import Optional
from contextlib import contextmanager
from .logger import logger


class DuckDBEngineError(Exception):
    """Raised when DuckDB engine encounters an error."""
    pass


class DuckDBEngine:
    """
    Managed DuckDB connection with proper lifecycle.
    
    Usage:
        with DuckDBEngine(threads=8, memory_limit="4GB") as con:
            result = con.execute("SELECT * FROM table").fetchdf()
    """
    
    def __init__(
        self,
        db_path: str = ":memory:",
        threads: int = 4,
        memory_limit: str = "2GB"
    ):
        """
        Initialize DuckDB engine configuration.
        
        Args:
            db_path: Path to DuckDB database file (":memory:" for in-memory)
            threads: Number of threads for query execution
            memory_limit: Memory limit (e.g., "2GB", "4GB")
        """
        self.db_path = db_path
        self.threads = threads
        self.memory_limit = memory_limit
        self.con: Optional[duckdb.DuckDBPyConnection] = None
        
    def __enter__(self) -> duckdb.DuckDBPyConnection:
        """Enter context manager - create and configure connection."""
        logger.info(f"Initializing DuckDB engine (threads={self.threads}, memory={self.memory_limit})")
        
        try:
            self.con = duckdb.connect(self.db_path)
            self._configure()
            return self.con
        except Exception as e:
            logger.error(f"Failed to initialize DuckDB engine: {e}")
            raise DuckDBEngineError(f"Failed to initialize DuckDB: {e}") from e
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit context manager - close connection."""
        if self.con:
            try:
                self.con.close()
                logger.debug("DuckDB connection closed")
            except Exception as e:
                logger.warning(f"Error closing DuckDB connection: {e}")
            
    def _configure(self):
        """Configure DuckDB with extensions and settings."""
        # Load required extensions
        required_extensions = ["postgres", "httpfs"]
        optional_extensions = ["snowflake"]
        
        for ext in required_extensions:
            try:
                logger.debug(f"Installing extension: {ext}")
                self.con.execute(f"INSTALL {ext};")
                self.con.execute(f"LOAD {ext};")
                logger.info(f"Loaded extension: {ext}")
            except Exception as e:
                logger.error(f"Failed to load required extension {ext}: {e}")
                raise DuckDBEngineError(
                    f"Required extension {ext} failed to load. "
                    f"This extension is needed for the pipeline to function."
                ) from e
        
        for ext in optional_extensions:
            try:
                logger.debug(f"Installing optional extension: {ext}")
                self.con.execute(f"INSTALL {ext};")
                self.con.execute(f"LOAD {ext};")
                logger.info(f"Loaded optional extension: {ext}")
            except Exception as e:
                logger.warning(f"Could not load optional extension {ext}: {e}")
        
        # Configure S3 access
        self._configure_s3()
        
        # Set performance parameters
        try:
            self.con.execute(f"PRAGMA threads={self.threads};")
            self.con.execute(f"SET memory_limit='{self.memory_limit}';")
            logger.debug(f"Set threads={self.threads}, memory_limit={self.memory_limit}")
        except Exception as e:
            logger.error(f"Failed to set DuckDB parameters: {e}")
            raise DuckDBEngineError(f"Failed to configure DuckDB: {e}") from e
        
    def _configure_s3(self):
        """Configure S3 access with proper credential handling."""
        region = os.getenv("AWS_REGION", "us-east-1")
        access_key = os.getenv("AWS_ACCESS_KEY_ID")
        secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        
        try:
            # Set S3 region
            self.con.execute(f"SET s3_region='{region}';")
            
            if access_key and secret_key:
                # Use credentials if available
                self.con.execute(f"SET s3_access_key_id='{access_key}';")
                self.con.execute(f"SET s3_secret_access_key='{secret_key}';")
                logger.info("Configured S3 with access key credentials")
            else:
                # Will use IAM role if running in AWS
                logger.info("S3 credentials not found - will attempt to use IAM role")
        except Exception as e:
            logger.warning(f"S3 configuration failed: {e}. S3 access may not work.")


@contextmanager
def make_con(
    db_path: str = ":memory:",
    threads: int = 4,
    memory_limit: str = "2GB"
):
    """
    Legacy context manager for backward compatibility.
    
    Use DuckDBEngine class instead:
        with DuckDBEngine(...) as con:
            ...
    """
    with DuckDBEngine(db_path, threads, memory_limit) as con:
        yield con


def resolve_env_tokens(s: str) -> str:
    """
    Legacy function for backward compatibility.
    
    Moved to config.py - import from there instead.
    """
    from .config import resolve_env_tokens as resolve
    return resolve(s)
