# DuckEL: DuckDB-powered Extract + Load POC

![CI](https://github.com/MrBisonte/quacknettor/actions/workflows/ci.yml/badge.svg)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

DuckEL is a lightweight, high-performance Extract and Load (EL) tool powered by **DuckDB**. It supports **Incremental Loading** (watermarking/upsert) and **Schema Evolution** across various sources and targets.

![DuckEL UI](assets/app_screenshot.png)

## Architecture

```mermaid
flowchart TD
    %% Use classes to style nodes
    classDef db fill:#e1f5fe,stroke:#01579b,stroke-width:2px;
    classDef file fill:#fff3e0,stroke:#e65100,stroke-width:2px;
    classDef core fill:#e8f5e9,stroke:#2e7d32,stroke-width:2px;
    classDef ui fill:#f3e5f5,stroke:#7b1fa2,stroke-width:2px,rx:10,ry:10;
    classDef ai fill:#fff,stroke:#ea4335,stroke-width:2px,stroke-dasharray: 5 5;

    User((🧑‍💻 User))

    subgraph App_Layer [Streamlit Application]
        direction TB
        UI[🖥️ Streamlit Frontend]:::ui
        Jules[🤖 Jules AI Assistant]:::ai
        Runner[⚙️ Pipeline Runner]:::core
    end

    subgraph Data_Sources [Source Layer]
        PG_Src[(🐘 Postgres)]:::db
        SF_Src[(❄️ Snowflake)]:::db
        PQ_Src{{📜 Parquet Files}}:::file
    end

    subgraph DuckDB_Engine [DuckDB Embedded Engine]
        Attach([🔗 Attach / Read])
        Schema([🧬 Schema Sync])
        Compute([⚡ Count & Sample])
        Write([💾 Write / Upsert])
    end

    subgraph Targets [Destination Layer]
        PG_Out[(🐘 Postgres)]:::db
        SF_Out[(❄️ Snowflake)]:::db
        PQ_Out{{📜 Parquet Files}}:::file
    end

    %% Interactions
    User -->|Config & Run| UI
    User -.->|Ask for Help| Jules
    Jules -.->|Suggest Pipeline| UI
    UI -->|Execute| Runner
    Runner -->|Orchestrate| DuckDB_Engine

    %% Data Flow
    PG_Src & SF_Src & PQ_Src ==> Attach
    Attach ==> Schema
    Schema ==> Compute
    Compute ==> Write
    Write ==> PG_Out & SF_Out & PQ_Out
```

### Key Components

*   **Incremental Loading**: Track the last watermark (e.g., `updated_at` column) to only load new or modified records. Supports `append` and `upsert` modes.
*   **Schema Evolution**: Automatically detect source schema changes and evolve the target table (e.g., adding missing columns).
*   **Adapters**: Native support for **Postgres**, **Snowflake**, and **Parquet/CSV** (Local or S3).
*   **Observability**: Integrated log viewer and real-time execution metrics.

### Setup

1.  **Clone & Install**:
    ```bash
    git clone https://github.com/MrBisonte/quacknettor.git
    cd quacknettor
    pip install .
    ```

2.  **Configure Environment**:
    Export variables for your databases (e.g., `PG_PASSWORD`, `SF_PASSWORD`). For AI help, set `JULES_API_KEY`.

3.  **Run the App**:
    ```bash
    streamlit run ui/main.py
    ```

## Usage

1.  **Select Pipeline**: Choose from definitions in `pipelines.yml`.
2.  **Configure Stages**: Toggle row counts, sampling, or summary statistics.
3.  **Incremental Controls**: If a pipeline supports incremental keys, you'll see options for **Full Refresh** and the current watermark.
4.  **Schema Support**: Select whether to `ignore`, `fail`, or `evolve` on schema mismatches.
5.  **Execute**: Watch the progress bar and analyze the results in the tabs.
6.  **AI Assistant**: Use the "AI Assistant (Jules)" tab to generate new pipeline YAML or get architectural advice.

## Project Structure

*   `ui/main.py`: Main Streamlit UI.
*   `configs/`: Centralized configuration directory.
    *   `configs/pipelines.yml`: Pipeline configuration definitions.
    *   `configs/pipelines_integration.yml`: Full matrix of 16 integration test pipelines.
    *   `configs/templates/`: Environment templates.
*   `duckel/`: Core engine code (Adapters, Models, Runner).
*   `tests/`: Unit verification suite.
*   `scripts/`: Utility scripts (data generation).
*   `logs/`: Execution logs and history.
*   `docker-compose.yml`: Development infrastructure (Postgres, MinIO/S3).

---

## Integration Testing

This section covers running the full integration test suite across all supported source/target combinations.

### Prerequisites

*   **Docker & Docker Compose**: Required for Postgres and MinIO (S3-compatible) services.
*   **Python 3.9+**: With project dependencies installed.
*   **Snowflake Account** (optional): Required only for Snowflake-related pipelines.

### 1. Environment Setup

#### Step 1: Start Docker Services

```bash
docker-compose up -d
```

This starts:
*   **Postgres** on `localhost:5432` (user: `testuser`, pass: `testpass`, db: `testdb`)
*   **MinIO** on `localhost:9000` (access: `minioadmin`, secret: `minioadmin`, bucket: `testbucket`)

#### Step 2: Configure Environment Variables

Copy `configs/templates/.env.template` to `.env` and fill in values:

```bash
cp configs/templates/.env.template .env
```

**Required for Postgres/S3 (Docker)**:
```env
AWS_ACCESS_KEY_ID=minioadmin
AWS_SECRET_ACCESS_KEY=minioadmin
S3_ENDPOINT=http://localhost:9000
DUCKEL_PG_PASSWORD=testpass
```

**Required for Snowflake (optional)**:
```env
SF_USER=your_username
SF_PASSWORD=your_password
SF_ACCOUNT=your_account
SF_WAREHOUSE=your_warehouse
SF_DATABASE=your_database
SF_SCHEMA=PUBLIC
```

#### Step 3: Generate & Seed Test Data

```bash
python scripts/generate_data.py
```

This creates test data with diverse datatypes (integers, floats, strings, booleans, dates, timestamps) and seeds it to local files, Postgres, and S3.

### 2. Running Pipelines

#### CLI Execution

Run a specific pipeline from `pipelines_integration.yml`:

```bash
python -c "
from duckel.config import load_config
from duckel.runner import PipelineRunner
import os

pipelines = load_config(os.path.join('configs', 'pipelines_integration.yml'))
config = pipelines['integration_local_to_postgres']
runner = PipelineRunner(config, pipeline_name='integration_local_to_postgres')
result = runner.run()
print(f'Rows processed: {result[\"rows\"]}')"
```

#### UI Execution

Launch the Streamlit app and select a pipeline from the dropdown:

```bash
streamlit run ui/main.py
```

> **Note**: To see integration pipelines in the UI, modify `ui/main.py` to load `configs/pipelines_integration.yml` instead of `configs/pipelines.yml`.

### 3. Running Integration Tests

**Docker-only pipelines** (Postgres + S3):
```bash
pytest tests/integration -m "integration and not snowflake" -v
```

**All pipelines** (requires Snowflake credentials):
```bash
pytest tests/integration -m integration -v
```

### 4. Pipeline Matrix

The integration suite covers **16 pipelines** for all combinations:

| Source ↓ / Target → | Local Parquet | S3 Parquet | Postgres | Snowflake |
|---------------------|---------------|------------|----------|-----------|
| **Local Parquet**   | ✅            | ✅         | ✅       | ✅        |
| **S3 Parquet**      | ✅            | ✅         | ✅       | ✅        |
| **Postgres**        | ✅            | ✅         | ✅       | ✅        |
| **Snowflake**       | ✅            | ✅         | ✅       | ✅        |

---

## Maintainers
- **bisontezao@outlook.com**

This is a community-developed tool, provided as-is. It comes with no official support or warranty. However, feel free to raise a GitHub issue if you find a bug or would like to suggest a new feature.

## Third Party Packages
DuckEL's functionality is powered by the following incredible third-party packages and the communities that maintain them:
- [DuckDB](https://duckdb.org/) - The core analytical engine.
- [Pandas](https://pandas.pydata.org/) - Data manipulation and analysis.
- [Streamlit](https://streamlit.io/) - Fast and beautiful UI framework.
- [Pydantic](https://docs.pydantic.dev/) - Data validation and settings management.
- [PyYAML](https://pyyaml.org/) - YAML parser and emitter.
- [Boto3](https://aws.amazon.com/sdk-for-python/) - AWS SDK for Python (S3 support).
- [Tenacity](https://tenacity.readthedocs.io/) - Retrying library for resilience.
- [Pytest](https://docs.pytest.org/en/latest/) - Testing framework.

## Legal
Licensed under the **MIT License**. You may not use this tool except in compliance with the License. You may obtain a copy of the License in the [LICENSE](LICENSE) file or at: [https://opensource.org/licenses/MIT](https://opensource.org/licenses/MIT)

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
