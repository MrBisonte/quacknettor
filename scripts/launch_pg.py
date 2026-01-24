import subprocess
import time
import sys
import os
from pathlib import Path

PG_BIN = r"C:\Program Files\PostgreSQL\15\bin"
PG_DATA = Path("data/pgdata").absolute()
PG_PORT = 5434
PG_USER = "testuser"
PG_PASS = "testpass"
PG_DB = "testdb"

def launch():
    print("Launching local PostgreSQL instance (standard binaries)...")
    
    if not os.path.exists(PG_BIN):
        print(f"Error: PostgreSQL binaries not found at {PG_BIN}")
        return

    # Check if data dir exists, if not init it (though we did it already)
    if not os.path.exists(PG_DATA):
        print("Initializing data directory...")
        os.makedirs(PG_DATA, exist_ok=True)
        with open("data/pgpass.txt", "w") as f:
            f.write(PG_PASS)
        
        cmd = [
            os.path.join(PG_BIN, "initdb.exe"),
            "-D", str(PG_DATA),
            "-U", PG_USER,
            "--pwfile=data/pgpass.txt"
        ]
        subprocess.run(cmd, check=True)

    print(f"Starting PostgreSQL on port {PG_PORT}...")
    # Generate a simple log file
    log_file = Path("logs/pg_server.log").absolute()
    os.makedirs(log_file.parent, exist_ok=True)
    
    # Use pg_ctl to start
    # We pass the port via -o
    cmd = [
        os.path.join(PG_BIN, "pg_ctl.exe"),
        "-D", str(PG_DATA),
        "-o", f"-p {PG_PORT}",
        "-l", str(log_file),
        "start"
    ]
    
    try:
        subprocess.run(cmd, check=True)
        print("\nPostgreSQL is up and running!")
        print("-" * 40)
        print(f"Host:     localhost")
        print(f"Port:     {PG_PORT}")
        print(f"User:     {PG_USER}")
        print(f"Pass:     {PG_PASS}")
        print(f"Database: postgres (default)")
        print("-" * 40)
        print("\nTo create your test database, you can run:")
        print(f'psql -h localhost -p {PG_PORT} -U {PG_USER} -c "CREATE DATABASE {PG_DB};"')
        print("-" * 40)
        print("\nConfiguration for DuckEL pipelines.yml:")
        print(f'conn: "dbname={PG_DB} user={PG_USER} host=localhost port={PG_PORT} password={PG_PASS}"')
        print("-" * 40)
        
        print("\nThe server is running in the background.")
        print("To stop it, run: pg_ctl -D data/pgdata stop")
        
    except Exception as e:
        print(f"Failed to start PostgreSQL: {e}")
        sys.exit(1)

if __name__ == "__main__":
    launch()
