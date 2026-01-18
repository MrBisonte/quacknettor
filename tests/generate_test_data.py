"""
Test data generator for integration tests.

Creates Parquet, CSV, and database tables containing diverse datatypes
to verify type fidelity across all source/target combinations.
"""
import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path

# Data directory
DATA_DIR = Path(__file__).parent / "data"


def generate_test_dataframe(num_rows: int = 100) -> pd.DataFrame:
    """
    Generate a DataFrame with diverse datatypes for testing.
    
    Datatypes covered:
    - Integer (int64)
    - Float (float64)
    - String (object)
    - Boolean (bool)
    - Timestamp (datetime64[ns])
    - Date (datetime64[ns] - date only)
    """
    np.random.seed(42)  # Reproducibility
    
    base_date = datetime(2024, 1, 1)
    
    data = {
        "id": list(range(1, num_rows + 1)),
        "int_col": np.random.randint(0, 10000, size=num_rows),
        "float_col": np.random.uniform(0.0, 1000.0, size=num_rows).round(4),
        "string_col": [f"row_{i}_data" for i in range(num_rows)],
        "bool_col": np.random.choice([True, False], size=num_rows),
        "timestamp_col": [base_date + timedelta(seconds=i * 3600) for i in range(num_rows)],
        "date_col": [(base_date + timedelta(days=i % 365)).date() for i in range(num_rows)],
    }
    
    df = pd.DataFrame(data)
    
    # Set explicit types
    df["id"] = df["id"].astype("int64")
    df["int_col"] = df["int_col"].astype("int64")
    df["float_col"] = df["float_col"].astype("float64")
    df["bool_col"] = df["bool_col"].astype("bool")
    df["timestamp_col"] = pd.to_datetime(df["timestamp_col"])
    df["date_col"] = pd.to_datetime(df["date_col"])
    
    return df


def save_parquet(df: pd.DataFrame, filename: str = "test_data.parquet"):
    """Save DataFrame to Parquet file."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    path = DATA_DIR / filename
    df.to_parquet(path, index=False, engine="pyarrow")
    print(f"Saved Parquet: {path}")
    return path


def save_csv(df: pd.DataFrame, filename: str = "test_data.csv"):
    """Save DataFrame to CSV file."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    path = DATA_DIR / filename
    df.to_csv(path, index=False)
    print(f"Saved CSV: {path}")
    return path


def seed_postgres(df: pd.DataFrame, table_name: str = "test_source"):
    """Seed test data into Postgres."""
    import psycopg2
    from psycopg2.extras import execute_values
    
    conn_str = os.getenv(
        "PG_CONN_STR",
        "host=localhost port=5432 dbname=testdb user=testuser password=testpass"
    )
    
    conn = psycopg2.connect(conn_str)
    cur = conn.cursor()
    
    # Create table
    cur.execute(f"""
        DROP TABLE IF EXISTS {table_name};
        CREATE TABLE {table_name} (
            id BIGINT PRIMARY KEY,
            int_col BIGINT,
            float_col DOUBLE PRECISION,
            string_col TEXT,
            bool_col BOOLEAN,
            timestamp_col TIMESTAMP,
            date_col DATE
        );
    """)
    
    # Insert data
    cols = ["id", "int_col", "float_col", "string_col", "bool_col", "timestamp_col", "date_col"]
    values = [tuple(row) for row in df[cols].values]
    execute_values(cur, f"INSERT INTO {table_name} ({', '.join(cols)}) VALUES %s", values)
    
    conn.commit()
    cur.close()
    conn.close()
    
    print(f"Seeded Postgres table: {table_name} ({len(df)} rows)")


def upload_to_minio(filepath: Path, bucket: str = "testbucket", object_name: str = None):
    """Upload a file to MinIO (S3 compatible)."""
    import boto3
    from botocore.client import Config
    
    s3 = boto3.client(
        "s3",
        endpoint_url=os.getenv("S3_ENDPOINT", "http://localhost:9000"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),
        config=Config(signature_version="s3v4"),
        region_name="us-east-1"
    )
    
    object_name = object_name or filepath.name
    s3.upload_file(str(filepath), bucket, object_name)
    print(f"Uploaded to S3: s3://{bucket}/{object_name}")
    return f"s3://{bucket}/{object_name}"


def main():
    """Generate all test data."""
    print("Generating test data...")
    
    df = generate_test_dataframe(100)
    
    # Local files
    parquet_path = save_parquet(df)
    save_csv(df)
    
    # Optionally seed to services if available
    try:
        seed_postgres(df)
    except Exception as e:
        print(f"Skipping Postgres seed: {e}")
    
    try:
        upload_to_minio(parquet_path)
    except Exception as e:
        print(f"Skipping S3 upload: {e}")
    
    print("Done!")


if __name__ == "__main__":
    main()
