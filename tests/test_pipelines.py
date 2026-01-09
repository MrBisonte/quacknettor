import os
import pytest
import duckdb
import pandas as pd
import boto3
from urllib.parse import urlparse
from duckel.runner import run_pipeline, PipelineRunner
from duckel.models import PipelineConfig

# Note: Tests for Postgres and Snowflake pipelines are pending.
# These will be added in a future iteration.

@pytest.fixture(scope="module")
def setup_data():
    """Create sample data for testing."""
    data_dir = "./data/inbound"
    os.makedirs(data_dir, exist_ok=True)

    df = pd.DataFrame({
        "a": [1, 2, 3],
        "b": ["x", "y", "z"]
    })

    file_path = f"{data_dir}/test_data.parquet"
    df.to_parquet(file_path, index=False)

    yield file_path

    if os.path.exists(file_path):
        os.remove(file_path)

def test_local_parquet_to_parquet(setup_data):
    """Test the local Parquet to Parquet pipeline using new PipelineRunner."""
    in_path = setup_data
    out_dir = "./data/outbound"
    os.makedirs(out_dir, exist_ok=True)
    out_path = f"{out_dir}/test_data_copy.parquet"

    # Test with new PipelineRunner class
    config = PipelineConfig(
        source={"type": "parquet", "path": in_path},
        target={"type": "parquet", "path": out_path, "mode": "overwrite"}
    )
    
    runner = PipelineRunner(config)
    result = runner.run()

    assert result["rows"] == 3
    assert os.path.exists(out_path)

    df = pd.read_parquet(out_path)
    assert len(df) == 3

    os.remove(out_path)

# S3 tests are disabled by default to avoid dependency on credentials.
# To run these, set the S3_TEST_BUCKET environment variable.
S3_TEST_BUCKET = os.environ.get("S3_TEST_BUCKET")

@pytest.mark.skipif(not S3_TEST_BUCKET, reason="S3_TEST_BUCKET not set")
def test_s3_parquet_to_local(setup_data):
    """Test S3 to local Parquet pipeline."""
    in_path = setup_data
    s3_path = f"s3://{S3_TEST_BUCKET}/in/test_data.parquet"
    out_path = "./data/outbound/s3_test_data_copy.parquet"

    # Upload to S3
    con = duckdb.connect()
    con.execute(f"COPY (SELECT * FROM read_parquet('{in_path}')) TO '{s3_path}' (FORMAT parquet);")

    pipeline = {
        "source": {"type": "parquet", "path": s3_path},
        "target": {"type": "parquet", "path": out_path, "mode": "overwrite"}
    }

    result = run_pipeline(pipeline)

    assert result["rows"] == 3
    assert os.path.exists(out_path)

    df = pd.read_parquet(out_path)
    assert len(df) == 3

    os.remove(out_path)

    # Clean up S3
    parsed_url = urlparse(s3_path)
    s3 = boto3.client("s3")
    s3.delete_object(Bucket=parsed_url.netloc, Key=parsed_url.path.lstrip('/'))

@pytest.mark.skipif(not S3_TEST_BUCKET, reason="S3_TEST_BUCKET not set")
def test_local_parquet_to_s3(setup_data):
    """Test local to S3 Parquet pipeline."""
    in_path = setup_data
    s3_path = f"s3://{S3_TEST_BUCKET}/out/test_data_copy.parquet"

    pipeline = {
        "source": {"type": "parquet", "path": in_path},
        "target": {"type": "parquet", "path": s3_path, "mode": "overwrite"}
    }

    result = run_pipeline(pipeline)

    assert result["rows"] == 3

    # Verify the file exists on S3
    con = duckdb.connect()
    df = con.execute(f"SELECT * FROM read_parquet('{s3_path}');").fetchdf()
    assert len(df) == 3

    # Clean up S3
    parsed_url = urlparse(s3_path)
    s3 = boto3.client("s3")
    s3.delete_object(Bucket=parsed_url.netloc, Key=parsed_url.path.lstrip('/'))
