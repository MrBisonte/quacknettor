# Quacknettor Naming Conventions (v1.0)

This document defines the naming standards for the Quacknettor project to ensure consistency across the backend, frontend, and data layers.

## 1. General Repository Conventions

### Branch Naming
Use prefixes to categorize the purpose of the branch:
- `feature/`: New functionality (e.g., `feature/snowflake-adapter`)
- `fix/`: Bug fixes (e.g., `fix/pg-connection-timeout`)
- `release/`: Preparation for a new release (e.g., `release/v1.0.0`)
- `hotfix/`: Critical fixes for production (e.g., `hotfix/security-patch`)

### Commit Messages
Follow the [Conventional Commits](https://www.conventionalcommits.org/) specification:
- `feat`: A new feature
- `fix`: A bug fix
- `docs`: Documentation only changes
- `style`: Changes that do not affect the meaning of the code (white-space, formatting, missing semi-colons, etc)
- `refactor`: A code change that neither fixes a bug nor adds a feature
- `test`: Adding missing tests or correcting existing tests
- `chore`: Changes to the build process or auxiliary tools and libraries

**Example:** `feat(adapter): add support for Snowflake upsert`

### Versioning
Use [Semantic Versioning (SemVer)](https://semver.org/): `vMAJOR.MINOR.PATCH` (e.g., `v1.0.0`).

---

## 2. Backend (Python)

Quacknettor follows [PEP 8](https://peps.python.org/pep-0008/) with the following specifics:

- **Files/Modules**: `snake_case.py` (e.g., `adapters.py`, `pipeline_runner.py`).
- **Classes**: `PascalCase` (e.g., `PostgresSourceAdapter`, `PipelineConfig`).
- **Functions & Methods**: `snake_case` (e.g., `get_relation_sql()`, `validate_config()`).
- **Variables & Arguments**: `snake_case` (e.g., `sql_query`, `connection_str`).
- **Constants**: `UPPER_SNAKE_CASE` (e.g., `DEFAULT_THREADS`, `MAX_RETRY_COUNT`).
- **Private Members**: Prefix with a single underscore (e.g., `_sanitize_identifier`).
- **Type Hinting**: Required for all public functions/methods.

---

## 3. Frontend (React / Next.js)

- **Component Files**: `PascalCase.tsx` (e.g., `CreatePipelineModal.tsx`, `LogViewer.tsx`).
- **Pages & Folders**: `lowercase-kebab` (e.g., `/dashboard`, `/pipeline-details`).
- **Hooks**: `camelCase` starting with `use` (e.g., `usePipelineStatus`).
- **Props & State**: `camelCase` (e.g., `isLoading`, `pipelineConfig`).
- **CSS Classes**: Use BEM (Block-Element-Modifier) pattern if not using CSS Modules.
    - `block__element--modifier`

---

## 4. Data & SQL

- **SQL Keywords**: Always `UPPERCASE` (e.g., `SELECT`, `FROM`, `JOIN`, `WHERE`).
- **Table & Column Names**: `snake_case` (e.g., `raw_transactions`, `updated_at`).
- **S3 Paths**: Use Hive-style partitioning:
    - `s3://bucket/table/year=YYYY/month=MM/day=DD/file.parquet`
- **DuckDB Aliases**: `snake_case` (e.g., `SELECT count(*) as row_count`).

---

## 5. CI/CD & Scripts

- **GitHub Workflows**: `snake_case.yml` (e.g., `ci_pipeline.yml`).
- **Utility Scripts**: `snake_case.py` (e.g., `generate_data.py`).
- **Docker Images**: `lowercase-kebab` (e.g., `quacknettor-api`).
