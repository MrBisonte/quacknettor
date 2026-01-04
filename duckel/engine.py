import os
import re
import logging
import duckdb

logger = logging.getLogger(__name__)

def resolve_env_tokens(s: str) -> str:
    """
    Resolve environment variable tokens in a string.
    Replaces '__ENV:VAR_NAME' with the value of the environment variable VAR_NAME.
    Supports multiple tokens and tokens embedded in strings.
    """
    if not isinstance(s, str):
        return s
    
    # Pattern: __ENV: followed by alphanumeric/underscore characters
    pattern = r"__ENV:([A-Za-z0-9_]+)"
    
    def replace_match(match):
        var_name = match.group(1)
        val = os.environ.get(var_name)
        if val is None:
            logger.warning(f"Environment variable '{var_name}' not set, resolving to empty string.")
            return ""
        return val

    return re.sub(pattern, replace_match, s)

def make_con(db_path: str = ":memory:", threads: int = 4, memory_limit: str = "2GB") -> duckdb.DuckDBPyConnection:
    """
    Create and configure a DuckDB connection with necessary extensions loaded.
    """
    logger.info(f"Initializing DuckDB connection (threads={threads}, memory_limit={memory_limit})")
    con = duckdb.connect(db_path)
    
    # Extensions to load
    extensions = ["postgres", "httpfs", "snowflake"]
    
    for ext in extensions:
        try:
            con.execute(f"INSTALL {ext}; LOAD {ext};")
            logger.debug(f"Loaded extension: {ext}")
        except Exception as e:
            # Snowflake extension often fails in certain envs (version mismatch, etc.)
            logger.warning(f"Could not load extension '{ext}': {e}")
            
    con.execute(f"PRAGMA threads={int(threads)};")
    con.execute(f"SET memory_limit='{memory_limit}';")
    return con
