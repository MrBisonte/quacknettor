"""
Shared pytest fixtures for integration tests.

These fixtures manage Docker Compose lifecycle, database connections,
and S3 clients for end-to-end testing across all source/target combinations.
"""
import os
import time
import pytest
import subprocess
from pathlib import Path

# Project root
PROJECT_ROOT = Path(__file__).parent.parent


@pytest.fixture(scope="session")
def docker_services():
    """
    Start Docker Compose services before tests and tear down after.
    
    This fixture starts Postgres and MinIO containers, waits for them to
    be healthy, and shuts them down after all tests complete.
    """
    compose_file = PROJECT_ROOT / "docker-compose.yml"
    
    if not compose_file.exists():
        pytest.skip("docker-compose.yml not found")
    
    # Start services
    print("\n[Fixture] Starting Docker Compose services...")
    subprocess.run(
        ["docker-compose", "-f", str(compose_file), "up", "-d", "--wait"],
        check=True,
        cwd=PROJECT_ROOT
    )
    
    # Wait for services to be ready
    _wait_for_postgres()
    _wait_for_minio()
    
    yield  # Run tests
    
    # Teardown
    print("\n[Fixture] Stopping Docker Compose services...")
    subprocess.run(
        ["docker-compose", "-f", str(compose_file), "down", "-v"],
        check=False,
        cwd=PROJECT_ROOT
    )


def _wait_for_postgres(max_retries: int = 30, delay: float = 1.0):
    """Wait for Postgres to be ready."""
    import psycopg2
    
    conn_str = "host=localhost port=5432 dbname=testdb user=testuser password=testpass"
    
    for i in range(max_retries):
        try:
            conn = psycopg2.connect(conn_str)
            conn.close()
            print(f"[Fixture] Postgres is ready after {i + 1} attempts")
            return
        except psycopg2.OperationalError:
            time.sleep(delay)
    
    raise RuntimeError("Postgres did not become ready in time")


def _wait_for_minio(max_retries: int = 30, delay: float = 1.0):
    """Wait for MinIO to be ready."""
    import requests
    
    url = os.getenv("S3_ENDPOINT", "http://localhost:9000") + "/minio/health/live"
    
    for i in range(max_retries):
        try:
            resp = requests.get(url, timeout=2)
            if resp.status_code == 200:
                print(f"[Fixture] MinIO is ready after {i + 1} attempts")
                return
        except requests.RequestException:
            pass
        time.sleep(delay)
    
    raise RuntimeError("MinIO did not become ready in time")


@pytest.fixture(scope="session")
def postgres_connection(docker_services):
    """Provide a Postgres connection for tests."""
    import psycopg2
    
    conn_str = "host=localhost port=5432 dbname=testdb user=testuser password=testpass"
    conn = psycopg2.connect(conn_str)
    yield conn
    conn.close()


@pytest.fixture(scope="session")
def s3_client(docker_services):
    """Provide an S3 client (MinIO) for tests."""
    import boto3
    from botocore.client import Config
    
    client = boto3.client(
        "s3",
        endpoint_url=os.getenv("S3_ENDPOINT", "http://localhost:9000"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),
        config=Config(signature_version="s3v4"),
        region_name="us-east-1"
    )
    
    yield client


@pytest.fixture(scope="session")
def test_data(docker_services, postgres_connection, s3_client):
    """
    Generate and seed test data to all sources.
    
    This creates the test DataFrame, saves it locally, seeds Postgres,
    and uploads to S3.
    """
    from tests.generate_test_data import (
        generate_test_dataframe,
        save_parquet,
        seed_postgres,
        upload_to_minio
    )
    
    df = generate_test_dataframe(100)
    parquet_path = save_parquet(df, "integration_test_data.parquet")
    seed_postgres(df, "integration_source")
    upload_to_minio(parquet_path, "testbucket", "integration_test_data.parquet")
    
    return df


@pytest.fixture
def duckdb_env():
    """
    Set environment variables for DuckDB S3 access to MinIO.
    """
    original_env = os.environ.copy()
    
    os.environ["AWS_ACCESS_KEY_ID"] = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
    os.environ["AWS_SECRET_ACCESS_KEY"] = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
    os.environ["AWS_REGION"] = "us-east-1"
    # For MinIO, we need to set the endpoint
    os.environ["S3_ENDPOINT"] = os.getenv("S3_ENDPOINT", "http://localhost:9000")
    
    yield
    
    # Restore original
    os.environ.clear()
    os.environ.update(original_env)
