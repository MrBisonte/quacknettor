from typing import Dict

def build_source_relation_sql(source: Dict[str, str]) -> str:
    """
    Constructs the DuckDB relation SQL (e.g., 'read_parquet(...)') for a given source config.

    WARNING: This logic constructs SQL strings directly from configuration parameters. 
    Ensure source configuration is trusted to prevent SQL injection.
    """
    st = source["type"]

    if st == "postgres":
        # We attach under source["name"] and then read schema.table
        obj = source.get("object")
        query = source.get("query")
        if query:
            return f"({query})"
        return obj  # "public.table"

    if st == "parquet":
        return f"read_parquet('{source['path']}')"

    if st == "csv":
        # You can add delim/header/infer options as needed
        return f"read_csv_auto('{source['path']}')"

    if st == "duckdb":
        # local duckdb file, read table
        return source["object"]

    if st == "snowflake":
        # Placeholder: depends on snowflake extension SQL surface
        # Keep the POC focused: Snowflake -> Parquet is your proof
        obj = source.get("object")
        query = source.get("query")
        return f"({query})" if query else obj

    raise ValueError(f"Unsupported source type: {st}")


def build_target_write_sql(target: Dict[str, str], relation_sql: str) -> str:
    """
    Constructs the DuckDB write SQL (e.g., 'COPY ... TO ...') for a given target config.
    """
    tt = target["type"]

    if tt == "parquet":
        path = target["path"]
        compression = target.get("compression", "zstd")
        # Fast path: COPY TO parquet
        return f"COPY (SELECT * FROM {relation_sql}) TO '{path}' (FORMAT parquet, COMPRESSION {compression});"

    if tt == "csv":
        path = target["path"]
        return f"COPY (SELECT * FROM {relation_sql}) TO '{path}' (HEADER, DELIMITER ',');"

    if tt == "postgres":
        # We attach under target["name"], then CREATE/INSERT into table via duckdb
        table = target["table"]
        mode = target.get("mode", "append")

        if mode == "overwrite":
            return f"""
            DROP TABLE IF EXISTS {table};
            CREATE TABLE {table} AS SELECT * FROM {relation_sql};
            """
        else:
            return f"INSERT INTO {table} SELECT * FROM {relation_sql};"

    raise ValueError(f"Unsupported target type: {tt}")
