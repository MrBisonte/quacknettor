"""
Integration tests for pipeline runner.

Tests end-to-end pipeline execution with error handling.
"""
import pytest
import pandas as pd
import os
from pathlib import Path
from duckel.runner import PipelineRunner, PipelineExecutionError
from duckel.models import PipelineConfig


@pytest.fixture
def sample_parquet_file(tmp_path):
    """Create a sample Parquet file for testing."""
    df = pd.DataFrame({
        "id": [1, 2, 3, 4, 5],
        "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
        "value": [10.5, 20.3, 15.7, 30.1, 25.9]
    })
    
    file_path = tmp_path / "sample.parquet"
    df.to_parquet(file_path, index=False)
    
    return file_path


class TestPipelineRunnerSuccess:
    """Test successful pipeline execution."""
    
    def test_parquet_to_parquet(self, sample_parquet_file, tmp_path):
        """Test basic Parquet to Parquet pipeline."""
        output_path = tmp_path / "output.parquet"
        
        config = PipelineConfig(
            source={"type": "parquet", "path": str(sample_parquet_file)},
            target={"type": "parquet", "path": str(output_path), "mode": "overwrite"}
        )
        
        runner = PipelineRunner(config)
        result = runner.run()
        
        # Verify results
        assert result["rows"] == 5
        assert result["sample"] is not None
        assert len(result["sample"]) == 5
        assert "write_sql" in result
        assert "timings" in result
        
        # Verify output file was created
        assert output_path.exists()
        
        # Verify output data
        output_df = pd.read_parquet(output_path)
        assert len(output_df) == 5
        assert list(output_df.columns) == ["id", "name", "value"]
    
    def test_with_custom_options(self, sample_parquet_file, tmp_path):
        """Test pipeline with custom runtime options."""
        output_path = tmp_path / "output.parquet"
        
        config = PipelineConfig(
            source={"type": "parquet", "path": str(sample_parquet_file)},
            target={"type": "parquet", "path": str(output_path)}
        )
        
        overrides = {
            "compute_counts": True,
            "sample_data": True,
            "sample_rows": 3,
            "compute_summary": True,
            "threads": 2
        }
        
        runner = PipelineRunner(config, overrides)
        result = runner.run()
        
        # Verify sample size
        assert len(result["sample"]) == 3
        
        # Verify summary was generated
        assert result["summary"] is not None
        assert len(result["summary"]) > 0
    
    def test_disable_counts_and_sample(self, sample_parquet_file, tmp_path):
        """Test pipeline with counts and sample disabled."""
        output_path = tmp_path / "output.parquet"
        
        config = PipelineConfig(
            source={"type": "parquet", "path": str(sample_parquet_file)},
            target={"type": "parquet", "path": str(output_path)}
        )
        
        overrides = {
            "compute_counts": False,
            "sample_data": False
        }
        
        runner = PipelineRunner(config, overrides)
        result = runner.run()
        
        # Verify counts and sample are disabled
        assert result["rows"] == "N/A"
        assert result["sample"] is None
        
        # But output should still be created
        assert output_path.exists()
    
    def test_timing_metrics(self, sample_parquet_file, tmp_path):
        """Test that timing metrics are calculated."""
        output_path = tmp_path / "output.parquet"
        
        config = PipelineConfig(
            source={"type": "parquet", "path": str(sample_parquet_file)},
            target={"type": "parquet", "path": str(output_path)}
        )
        
        runner = PipelineRunner(config)
        result = runner.run()
        
        # Verify all timing metrics exist
        assert "count_s" in result["timings"]
        assert "sample_s" in result["timings"]
        assert "summary_s" in result["timings"]
        assert "write_s" in result["timings"]
        assert "total_s" in result["timings"]
        
        # Verify timings are non-negative
        for key, value in result["timings"].items():
            assert value >= 0


class TestPipelineRunnerErrors:
    """Test error handling in pipeline runner."""
    
    def test_missing_source_file(self, tmp_path):
        """Test that missing source file raises error."""
        output_path = tmp_path / "output.parquet"
        
        config = PipelineConfig(
            source={"type": "parquet", "path": "./nonexistent.parquet"},
            target={"type": "parquet", "path": str(output_path)}
        )
        
        runner = PipelineRunner(config)
        
        with pytest.raises(PipelineExecutionError):
            runner.run()
    
    def test_invalid_output_path(self, sample_parquet_file):
        """Test that invalid output path raises error."""
        config = PipelineConfig(
            source={"type": "parquet", "path": str(sample_parquet_file)},
            target={"type": "parquet", "path": "/invalid/path/output.parquet"}
        )
        
        runner = PipelineRunner(config)
        
        with pytest.raises(PipelineExecutionError):
            runner.run()
    
    def test_invalid_sql_in_source(self, tmp_path):
        """Test that source with bad SQL object is caught."""
        # This tests the sanitization in adapters
        output_path = tmp_path / "output.parquet"
        
        # Attempting to create config with SQL injection should fail at validation
        with pytest.raises(Exception):  # Could be ValidationError or ValueError
            config = PipelineConfig(
                source={
                    "type": "postgres",
                    "conn": "test",
                    "object": "users; DROP TABLE users;--"
                },
                target={"type": "parquet", "path": str(output_path)}
            )


class TestPipelineRunnerLegacyCompatibility:
    """Test backward compatibility with legacy run_pipeline function."""
    
    def test_legacy_run_pipeline_function(self, sample_parquet_file, tmp_path):
        """Test that legacy run_pipeline function still works."""
        from duckel.runner import run_pipeline
        
        output_path = tmp_path / "output.parquet"
        
        pipeline_dict = {
            "source": {"type": "parquet", "path": str(sample_parquet_file)},
            "target": {"type": "parquet", "path": str(output_path), "mode": "overwrite"}
        }
        
        result = run_pipeline(pipeline_dict)
        
        # Verify it works the same way
        assert result["rows"] == 5
        assert output_path.exists()


@pytest.mark.integration
class TestPipelineIntegration:
    """Integration tests with actual file operations."""
    
    def test_large_dataset(self, tmp_path):
        """Test pipeline with larger dataset."""
        # Create larger dataset
        df = pd.DataFrame({
            "id": range(1000),
            "value": range(1000, 2000)
        })
        
        input_path = tmp_path / "large_input.parquet"
        output_path = tmp_path / "large_output.parquet"
        df.to_parquet(input_path, index=False)
        
        config = PipelineConfig(
            source={"type": "parquet", "path": str(input_path)},
            target={"type": "parquet", "path": str(output_path)}
        )
        
        runner = PipelineRunner(config)
        result = runner.run()
        
        assert result["rows"] == 1000
        
        # Verify output
        output_df = pd.read_parquet(output_path)
        assert len(output_df) == 1000
    
    def test_compression_options(self, sample_parquet_file, tmp_path):
        """Test different compression options."""
        for compression in ["zstd", "gzip", "snappy"]:
            output_path = tmp_path / f"output_{compression}.parquet"
            
            config = PipelineConfig(
                source={"type": "parquet", "path": str(sample_parquet_file)},
                target={
                    "type": "parquet",
                    "path": str(output_path),
                    "compression": compression
                }
            )
            
            runner = PipelineRunner(config)
            result = runner.run()
            
            assert output_path.exists()
            assert compression.upper() in result["write_sql"]
