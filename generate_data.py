import pandas as pd
import numpy as np
import os

# Create data directory if not exists
os.makedirs("data", exist_ok=True)

# Generate sample Telco data
n_rows = 50
data = {
    "customer_id": [f"CUST-{i:04d}" for i in range(n_rows)],
    "gender": np.random.choice(["Male", "Female"], n_rows),
    "senior_citizen": np.random.choice([0, 1], n_rows),
    "tenure": np.random.randint(1, 72, n_rows),
    "service_type": np.random.choice(["DSL", "Fiber optic", "No"], n_rows),
    "contract": np.random.choice(["Month-to-month", "One year", "Two year"], n_rows),
    "monthly_charges": np.round(np.random.uniform(20, 120, n_rows), 2),
    "total_charges": np.round(np.random.uniform(100, 8000, n_rows), 2),
    "payment_method": np.random.choice(["Electronic check", "Mailed check", "Bank transfer", "Credit card"], n_rows),
    "churn": np.random.choice(["Yes", "No"], n_rows)
}

df = pd.DataFrame(data)

# Save to Parquet
output_path = "data/telco_churn_sample.parquet"
df.to_parquet(output_path, engine="pyarrow")

print(f"Generated {output_path} with {n_rows} rows and {len(df.columns)} columns.")
