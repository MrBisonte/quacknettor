import os
import duckdb
import logging
from duckel.engine import resolve_env_tokens

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("test_snowflake")

def test_connection():
    """
    Test DuckDB's ability to ATTACH to a Snowflake database.
    
    INSTRUCTIONS:
    1. Export the necessary environment variables (SF_ACCOUNT, SF_USER, SF_PASSWORD, etc.)
       OR edit the defaults below.
    """
    
    # Defaults (using tokens that resolve_env_tokens can handle if passed through engine, 
    # but here we construct the string directly for simplicity or use env vars).
    
    # We will construct the connection string from env vars directly to test the "happy path"
    user = os.environ.get("SF_USER", "user")
    password = os.environ.get("SF_PASSWORD", "password")
    account = os.environ.get("SF_ACCOUNT", "account") # e.g. xy12345.us-east-1
    warehouse = os.environ.get("SF_WAREHOUSE", "compute_wh")
    database = os.environ.get("SF_DATABASE", "demo_db")
    schema = os.environ.get("SF_SCHEMA", "public")
    
    conn_str = f"user={user} password={password} account={account} warehouse={warehouse} database={database} schema={schema}"
    
    logger.info(f"Testing connection to Snowflake account: {account}")
    
    con = duckdb.connect(":memory:")
    
    try:
        logger.info("Installing snowflake extension...")
        con.execute("INSTALL snowflake; LOAD snowflake;")
        
        logger.info("Attaching database...")
        # Note: TYPE snowflake
        con.execute(f"ATTACH '{conn_str}' AS sf_test (TYPE snowflake);")
        
        logger.info("Connection successful! Listing tables...")
        # Querying information schema from the attached database
        tables = con.execute("SELECT * FROM sf_test.information_schema.tables LIMIT 5;").fetchdf()
        print("\n--- Tables in Snowflake ---")
        print(tables)
        print("---------------------------\n")
        
        logger.info("Test passed.")
        
    except Exception as e:
        logger.error(f"Connection failed: {e}")
        logger.error("Hint: Check your SF_ACCOUNT format (should often be 'org-account' or 'locator.region').")
    finally:
        con.close()

if __name__ == "__main__":
    test_connection()
