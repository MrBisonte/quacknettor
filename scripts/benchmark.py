#!/usr/bin/env python3
import sys
import time

from duckel.config import load_config
from duckel.runner import PipelineRunner


def run_benchmark(config_path="configs/pipelines.yml", pipeline_name="local_parquet_to_parquet"):
    print(f"Loading {pipeline_name} from {config_path}...")
    try:
        pipelines = load_config(config_path)
    except Exception as e:
        print(f"Error loading configs: {e}")
        sys.exit(1)

    if pipeline_name not in pipelines:
        print(f"Pipeline {pipeline_name} not found. Available: {list(pipelines.keys())}")
        sys.exit(1)

    config = pipelines[pipeline_name]
    runner = PipelineRunner(config, pipeline_name=pipeline_name)

    print("\nStarting benchmark...")
    start_time = time.perf_counter()
    result = runner.run()
    end_time = time.perf_counter()

    duration = end_time - start_time
    rows = result.get("rows", 0)
    throughput = rows / duration if duration > 0 else 0

    print("\n" + "=" * 40)
    print("BENCHMARK RESULTS")
    print("=" * 40)
    print(f"Pipeline:   {pipeline_name}")
    print(f"Duration:   {duration:.4f} seconds")
    print(f"Rows Prcd:  {rows:,}")
    print(f"Throughput: {throughput:,.2f} rows/second")
    print("=" * 40)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="DuckEL Benchmarking Tool")
    parser.add_argument("--config", default="configs/pipelines.yml", help="Path to config file")
    parser.add_argument(
        "--pipeline", default="local_parquet_to_parquet", help="Pipeline name to benchmark"
    )
    args = parser.parse_args()

    run_benchmark(args.config, args.pipeline)
