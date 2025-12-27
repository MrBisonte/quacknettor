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
    for ext in ["postgres", "httpfs", "snowflake"]:
        try:
            con.execute(f"INSTALL {ext}; LOAD {ext};")
        except Exception as e:
            print(f"Warning: Could not load extension {ext}: {e}")

    # Set up S3 credentials. Prioritize IAM role, fall back to env vars.
    # boto3 will automatically handle the IAM role if available.
    s3_region = os.environ.get("AWS_REGION", "us-east-1")
    s3_access_key = os.environ.get("AWS_ACCESS_KEY_ID")
    s3_secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")

    con.execute(f"SET s3_region='{s3_region}';")
    if s3_access_key and s3_secret_key:
        con.execute(f"SET s3_access_key_id='{s3_access_key}';")
        con.execute(f"SET s3_secret_access_key='{s3_secret_key}';")

    con.execute(f"PRAGMA threads={int(threads)};")
    con.execute(f"SET memory_limit='{memory_limit}';")
    return con
