import time
from .engine import make_con, resolve_env_tokens
from .adapters import build_source_relation_sql, build_target_write_sql

def _attach_postgres(con, name: str, conn: str):
    conn = resolve_env_tokens(conn)
    con.execute(f"ATTACH '{conn}' AS {name} (TYPE postgres);")  # :contentReference[oaicite:5]{index=5}

def run_pipeline(p: dict, overrides: dict = None) -> dict:
    opts = p.get("options", {}).copy()
    if overrides:
        opts.update(overrides)

    con = make_con(
        threads=opts.get("threads", 4),
        memory_limit=opts.get("memory_limit", "2GB"),
    )

    source = p["source"]
    target = p["target"]

    # Attach Postgres if used
    if source["type"] == "postgres":
        _attach_postgres(con, source.get("name", "pgsrc"), source["conn"])
        # Qualify object with attachment name if user didn't
        if "object" in source and "." in source["object"]:
            relation_sql = f"{source.get('name','pgsrc')}.{source['object']}"
        else:
            relation_sql = build_source_relation_sql(source)

    elif source["type"] == "snowflake":
        # Re-use attach_postgres logic effectively, just changing TYPE
        conn_str = resolve_env_tokens(source["conn"])
        name = source.get("name", "sfsrc")
        # Ensure we don't have SQL injection issues or conflicts, though this is a POC
        con.execute(f"ATTACH '{conn_str}' AS {name} (TYPE snowflake);")
        
        if "object" in source:
             # If object is "db.schema.table", we might need "name.db.schema.table"
             # DuckDB snowflake attach maps the whole account? Or just the DB?
             # Usually "ATTACH '...' AS sf" makes "sf" accessible.
             # Inside sf, you have databases. So "sf.my_db.my_schema.my_table".
             # If the user provided "my_db.my_schema.my_table" in 'object', we prepend 'name'.
             if "." in source["object"]:
                relation_sql = f"{name}.{source['object']}"
             else:
                relation_sql = f"{name}.{source['object']}" # Assume it's a table?
        else:
             relation_sql = build_source_relation_sql(source)

    else:
        relation_sql = build_source_relation_sql(source)

    if target["type"] == "postgres":
        _attach_postgres(con, target.get("name", "pgtgt"), target["conn"])
        # Qualify target table with attachment name
        target = dict(target)
        if "." in target["table"]:
            target["table"] = f"{target.get('name','pgtgt')}.{target['table']}"

    # Metrics
    t0 = time.perf_counter()
    
    count = -1
    t_count = t0
    if opts.get("compute_counts", True):
        count = con.execute(f"SELECT COUNT(*) FROM {relation_sql};").fetchone()[0]
        t_count = time.perf_counter()

    sample = None
    t_sample = t_count
    if opts.get("sample_data", True):
        sample_n = int(opts.get("sample_rows", 50))
        sample = con.execute(f"SELECT * FROM {relation_sql} LIMIT {sample_n};").fetchdf()
        t_sample = time.perf_counter()

    summary = None
    t_summary = t_sample
    if opts.get("compute_summary", False):
        summary = con.execute(f"SUMMARIZE SELECT * FROM {relation_sql};").fetchdf()
        t_summary = time.perf_counter()

    write_sql = build_target_write_sql(target, relation_sql)
    con.execute(write_sql)
    t_write = time.perf_counter()

    return {
        "rows": count if count != -1 else "N/A",
        "sample": sample,
        "summary": summary,
        "timings": {
            "count_s": round(t_count - t0, 4) if opts.get("compute_counts", True) else 0,
            "sample_s": round(t_sample - t_count, 4) if opts.get("sample_data", True) else 0,
            "summary_s": round(t_summary - t_sample, 4) if opts.get("compute_summary", False) else 0,
            "write_s": round(t_write - t_summary, 4),
            "total_s": round(t_write - t0, 4),
        },
        "write_sql": write_sql.strip(),
    }
