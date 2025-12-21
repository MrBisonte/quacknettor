import os
import duckdb

def resolve_env_tokens(s: str) -> str:
    # Replace "__ENV:VAR" with os.environ["VAR"]
    if not isinstance(s, str):
        return s
    token = "__ENV:"
    if token in s:
        var = s.split(token, 1)[1]
        return s.replace(f"{token}{var}", os.environ.get(var, ""))
    return s

def make_con(db_path=":memory:", threads=4, memory_limit="2GB"):
    con = duckdb.connect(db_path)
    con.execute("INSTALL postgres; LOAD postgres;")  # ok even if already installed
    con.execute("INSTALL httpfs; LOAD httpfs;")      # for S3 later
    con.execute("INSTALL snowflake; LOAD snowflake;")
    con.execute(f"PRAGMA threads={int(threads)};")
    con.execute(f"SET memory_limit='{memory_limit}';")
    return con
