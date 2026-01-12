import pytest
import duckdb
import os
import json
from pathlib import Path
from duckel.runner import PipelineRunner
from duckel.models import PipelineConfig

def test_incremental_flow(tmp_path):
    # Setup source data (parquet)
    src_path = str(tmp_path / "source.parquet")
    tgt_path = str(tmp_path / "target.parquet")
    state_path = tmp_path / "state.json"
    
    con = duckdb.connect()
    # Create source with timestamp-like column
    con.execute("CREATE TABLE src (id INTEGER, val VARCHAR, updated_at INTEGER)")
    con.execute("INSERT INTO src VALUES (1, 'a', 100), (2, 'b', 200)")
    con.execute(f"COPY src TO '{src_path}' (FORMAT PARQUET)")
    
    config = {
        "source": {
            "type": "parquet", 
            "path": src_path, 
            "incremental_key": "updated_at"
        },
        "target": {
            "type": "parquet", 
            "path": tgt_path, 
            "mode": "append"
        },
        "options": {
            "compute_counts": True,
            "sample_data": False
        }
    }
    
    # Run 1: Should load all (start from 0 or None)
    pipeline_config = PipelineConfig(**config)
    runner = PipelineRunner(pipeline_config, pipeline_name="test_inc")
    
    # Mock state path
    runner._get_state_path = lambda: state_path
    
    res = runner.run()
    assert res['rows'] == 2, "First run should load all 2 rows"
    
    # Verify State
    assert state_path.exists()
    with open(state_path) as f:
        state = json.load(f)
        assert state["test_inc"]["watermark"] == 200
    
    # Run 2: No new data
    res2 = runner.run()
    assert res2['rows'] == 0, "Second run with no changes should load 0 rows"
    # Watermark should remain 200
    with open(state_path) as f:
        state = json.load(f)
        assert state["test_inc"]["watermark"] == 200
    
    # Add new data
    con.execute("INSERT INTO src VALUES (3, 'c', 300)")
    con.execute(f"COPY src TO '{src_path}' (FORMAT PARQUET)")
    
    # Run 3: Should load 1 row
    res3 = runner.run()
    assert res3['rows'] == 1, "Third run should load only the new row"
    
    # Verify new watermark
    with open(state_path) as f:
        state = json.load(f)
        assert state["test_inc"]["watermark"] == 300
    
    con.close()
