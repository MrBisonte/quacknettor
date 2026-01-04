import os
import duckdb
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("test_pg")

def test_connection():
    """
    Test DuckDB's ability to ATTACH to a Postgres database.
    
    INSTRUCTIONS:
    1. Set the PG_CONNECTION_STRING environment variable 
       OR edit the 'conn_str' variable below directly.
    """
    
    # Default is empty to force user to provide credentials
    host = os.environ.get("PG_HOST", "localhost")
    user = os.environ.get("PG_USER", "dbadmin")
    pw = os.environ.get("PG_PASSWORD", "adminpwd123")
    db = os.environ.get("PG_DATABASE", "dbmain")
    
    default_conn = f"dbname={db} user={user} password={pw} host={host} port=5432"
    conn_str = os.environ.get("PG_CONNECTION_STRING", default_conn)
    # --------------------------------

    logger.info(f"Testing connection to: {conn_str}")
    
    con = duckdb.connect(":memory:")
    
    try:
        logger.info("Installing postgres extension...")
        con.execute("INSTALL postgres; LOAD postgres;")
        
        logger.info("Attaching database...")
        con.execute(f"ATTACH '{conn_str}' AS pg_test (TYPE postgres);")
        
        logger.info("Connection successful! Listing tables...")
        tables = con.execute("SELECT * FROM information_schema.tables WHERE table_schema='public' LIMIT 5;").fetchdf()
        print("\n--- Tables in public schema ---")
        print(tables)
        print("-------------------------------\n")
        
        logger.info("Test passed.")
        
    except Exception as e:
        logger.error(f"Connection failed: {e}")
    finally:
        con.close()

if __name__ == "__main__":
    test_connection()
