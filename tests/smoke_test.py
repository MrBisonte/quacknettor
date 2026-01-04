import os
import sys
import logging
from duckel.config import load_config
from duckel.runner import run_pipeline

# Configure logging to see output
logging.basicConfig(level=logging.INFO)

def run_smoke_test():
    """
    Run the 'pg_local' -> 'parquet_local_out' pipeline programmatically.
    """
    base_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(base_dir)
    sys.path.append(project_root) # Ensure duckel is in pythonpath
    
    config_path = os.path.join(project_root, "pipelines.yml")
    print(f"Loading config from: {config_path}")
    cfg = load_config(config_path)
    
    # Helper to resolve relative paths (copied from app.py logic roughly)
    def resolve_paths(d):
        if isinstance(d, dict):
            for k, v in d.items():
                if k == "path" and isinstance(v, str) and v.startswith("."):
                    d[k] = os.path.join(project_root, v)
                else:
                    resolve_paths(v)
        elif isinstance(d, list):
            for item in d:
                resolve_paths(item)

    resolve_paths(cfg["sources"])
    resolve_paths(cfg["targets"])
    
    # Construct pipeline: Postgres Source -> Parquet Target
    source = cfg["sources"]["pg_local"]
    target = cfg["targets"]["parquet_local_out"]
    
    pipeline_def = {
        "source": source,
        "target": target,
        "options": {
            "threads": 2,
            "memory_limit": "500MB",
            "sample_rows": 10
        }
    }
    
    print("\n--- Running Smoke Test Pipeline ---")
    try:
        result = run_pipeline(pipeline_def, overrides={"compute_counts": True, "sample_data": True})
        print("Pipeline Result:")
        print(f"  Rows Processed: {result['rows']}")
        print(f"  Timings: {result['timings']}")
        print("  Sample Data (First row):")
        print(result['sample'].iloc[0].to_dict() if result['sample'] is not None else "No sample")
        print("\n[SUCCESS] Pipeline executed successfully.")
    except Exception as e:
        print(f"\n[FAILURE] Pipeline failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    run_smoke_test()
