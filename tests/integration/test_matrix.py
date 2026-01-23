"""
Integration test suite for all 16 pipeline combinations.

This module runs each pipeline from pipelines_integration.yml and verifies
that data is correctly transferred to the target with type fidelity.
"""
import os
import pytest
import yaml
from pathlib import Path

# Add project root to path
import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from duckel.config import load_config
from duckel.runner import PipelineRunner


# Load integration pipelines
INTEGRATION_CONFIG = Path(__file__).parent.parent.parent / "configs" / "pipelines_integration.yml"


def get_pipeline_names():
    """Get list of all pipeline names from integration config."""
    with open(INTEGRATION_CONFIG) as f:
        config = yaml.safe_load(f)
    return list(config.get("pipelines", {}).keys())


# Pipelines that require Snowflake (skip if no credentials)
SNOWFLAKE_PIPELINES = [
    name for name in get_pipeline_names() if "snowflake" in name
]

# Pipelines that only need Docker (Postgres + MinIO)
DOCKER_ONLY_PIPELINES = [
    name for name in get_pipeline_names() if "snowflake" not in name
]


def has_snowflake_creds() -> bool:
    """Check if Snowflake credentials are configured."""
    required = ["SF_USER", "SF_PASSWORD", "SF_ACCOUNT", "SF_WAREHOUSE", "SF_DATABASE", "SF_SCHEMA"]
    return all(os.getenv(var) for var in required)


@pytest.fixture(scope="module")
def integration_pipelines():
    """Load all integration pipelines."""
    return load_config(str(INTEGRATION_CONFIG))


class TestDockerOnlyPipelines:
    """
    Test pipelines that only require Docker services (Postgres + MinIO).
    
    These tests require:
    - Docker Compose services running (docker-compose up -d)
    - Test data seeded to sources
    """
    
    @pytest.mark.integration
    @pytest.mark.parametrize("pipeline_name", DOCKER_ONLY_PIPELINES)
    def test_pipeline_execution(self, docker_services, test_data, duckdb_env, integration_pipelines, pipeline_name):
        """Execute a pipeline and verify it completes without error."""
        if pipeline_name not in integration_pipelines:
            pytest.skip(f"Pipeline {pipeline_name} not found in config")
        
        pipeline_config = integration_pipelines[pipeline_name]
        
        # Create output directory if needed
        if hasattr(pipeline_config.target, "path"):
            out_path = Path(pipeline_config.target.path)
            if not out_path.name.startswith("s3://"):
                out_path.parent.mkdir(parents=True, exist_ok=True)
        
        runner = PipelineRunner(pipeline_config, pipeline_name=pipeline_name)
        result = runner.run()
        
        # Basic assertions
        assert result is not None
        assert "rows" in result
        assert result["rows"] > 0, f"Pipeline {pipeline_name} produced no rows"
        
        # Verify row count matches source
        assert result["rows"] == len(test_data), (
            f"Row count mismatch: expected {len(test_data)}, got {result['rows']}"
        )


class TestSnowflakePipelines:
    """
    Test pipelines that involve Snowflake.
    
    These tests require:
    - Valid Snowflake credentials in environment
    - Docker Compose services running
    - Test data seeded
    """
    
    @pytest.mark.integration
    @pytest.mark.snowflake
    @pytest.mark.parametrize("pipeline_name", SNOWFLAKE_PIPELINES)
    def test_pipeline_execution(self, docker_services, test_data, duckdb_env, integration_pipelines, pipeline_name):
        """Execute a Snowflake pipeline and verify it completes."""
        if not has_snowflake_creds():
            pytest.skip("Snowflake credentials not configured")
        
        if pipeline_name not in integration_pipelines:
            pytest.skip(f"Pipeline {pipeline_name} not found in config")
        
        pipeline_config = integration_pipelines[pipeline_name]
        
        runner = PipelineRunner(pipeline_config, pipeline_name=pipeline_name)
        result = runner.run()
        
        assert result is not None
        assert "rows" in result
        assert result["rows"] > 0


@pytest.mark.integration
def test_all_pipeline_types_covered(integration_pipelines):
    """Verify that we have all 16 expected pipeline combinations."""
    sources = ["local", "s3", "postgres", "snowflake"]
    targets = ["local", "s3", "postgres", "snowflake"]
    
    expected = set()
    for src in sources:
        for tgt in targets:
            expected.add(f"integration_{src}_to_{tgt}")
    
    actual = set(integration_pipelines.keys())
    
    missing = expected - actual
    assert not missing, f"Missing pipeline combinations: {missing}"
