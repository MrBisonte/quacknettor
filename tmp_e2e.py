import os
import sys
import pandas as pd
from duckel.config import load_config

print("1. Creating dummy test data...")
os.makedirs("data/inbound", exist_ok=True)
os.makedirs("data/outbound", exist_ok=True)
pd.DataFrame({"id": range(10000), "val": ["test"]*10000}).to_parquet("data/inbound/telco_churn_sample.parquet")

print("2. Testing configuration validation (ensuring env credentials match secrets policy)...")
try:
    pipelines = load_config("configs/pipelines.yml")
    print("[SUCCESS] All pipelines validated securely under new models.")
except Exception as e:
    print(f"[FAIL] Configuration validation failed: {e}")
    sys.exit(1)

print("\n3. Testing End-to-End Execution via Benchmark Script...")
ret = os.system(f"{sys.executable} scripts/benchmark.py --config configs/pipelines.yml --pipeline local_parquet_to_parquet")
if ret != 0:
    print("[FAIL] Benchmark script execution failed.")
    sys.exit(1)
else:
    print("[SUCCESS] End-to-End test completed.")
