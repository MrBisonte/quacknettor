import time
import os
import logging
from typing import Dict, Any, Optional

from .engine import make_con, resolve_env_tokens
from .adapters import build_source_relation_sql, build_target_write_sql

logger = logging.getLogger(__name__)

def _attach_postgres(con, name: str, conn: str) -> None:
    conn = resolve_env_tokens(conn)
    logger.info(f"Attaching Postgres database as '{name}'")
    con.execute(f"ATTACH '{conn}' AS {name} (TYPE postgres);")

def run_pipeline(p: Dict[str, Any], overrides: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Execute the pipeline defined by the configuration dictionary 'p'.
    
    Args:
        p: Pipeline configuration dictionary containing 'source', 'target', and 'options'.
        overrides: Optional dictionary to override operational settings (e.g. sample_rows).
        
    Returns:
        Dictionary containing execution metrics (rows, timings, etc.) and sample data.
    """
    opts = p.get("options", {}).copy()
    if overrides:
        opts.update(overrides)

    threads = int(opts.get("threads", 4))
    memory_limit = opts.get("memory_limit", "2GB")

    con = make_con(threads=threads, memory_limit=memory_limit)
    
    try:
        source = p["source"]
        target = p["target"].copy() # Copy to avoid mutating original config

        # Apply overrides to target if present
        if overrides and "target_table" in overrides:
             target["table"] = overrides["target_table"]

        # --- Source Setup ---
        logger.info(f"Setting up source: {source.get('type')}")
        
        if source["type"] == "postgres":
            _attach_postgres(con, source.get("name", "pgsrc"), source["conn"])
            # Qualify object with attachment name if user didn't
            if "object" in source and "." in source["object"]:
                relation_sql = f"{source.get('name','pgsrc')}.{source['object']}"
            else:
                relation_sql = build_source_relation_sql(source)

        elif source["type"] == "snowflake":
            conn_str = resolve_env_tokens(source["conn"])
            name = source.get("name", "sfsrc")
            logger.info(f"Attaching Snowflake database as '{name}'")
            con.execute(f"ATTACH '{conn_str}' AS {name} (TYPE snowflake);")
            
            if "object" in source:
                 if "." in source["object"]:
                    relation_sql = f"{name}.{source['object']}"
                 else:
                    relation_sql = f"{name}.{source['object']}"
            else:
                 relation_sql = build_source_relation_sql(source)  # Fallback often unlikely for SF
        else:
            relation_sql = build_source_relation_sql(source)

        # --- Target Setup ---
        if target["type"] == "postgres":
            _attach_postgres(con, target.get("name", "pgtgt"), target["conn"])
            target = dict(target)
            if "." in target["table"]:
                target["table"] = f"{target.get('name','pgtgt')}.{target['table']}"

        # --- Metrics & Execution ---
        t0 = time.perf_counter()
        
        count = -1
        t_count = t0
        if opts.get("compute_counts", True):
            logger.info("Computing row count...")
            count = con.execute(f"SELECT COUNT(*) FROM {relation_sql};").fetchone()[0]
            t_count = time.perf_counter()

        sample = None
        t_sample = t_count
        if opts.get("sample_data", True):
            logger.info("Sampling data...")
            sample_n = int(opts.get("sample_rows", 50))
            sample = con.execute(f"SELECT * FROM {relation_sql} LIMIT {sample_n};").fetchdf()
            t_sample = time.perf_counter()

        summary = None
        t_summary = t_sample
        if opts.get("compute_summary", False):
            logger.info("Computing summary...")
            summary = con.execute(f"SUMMARIZE SELECT * FROM {relation_sql};").fetchdf()
            t_summary = time.perf_counter()

        # Ensure target directory exists for file-based targets
        if "path" in target:
            out_dir = os.path.dirname(target["path"])
            if out_dir:
                os.makedirs(out_dir, exist_ok=True)

        logger.info(f"Writing to target: {target.get('type')}")
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
    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}", exc_info=True)
        raise e
    finally:
        logger.debug("Closing DuckDB connection")
        con.close()
