# Refactor: Implement Naming Convention Plan and Reorganize Project Structure

## Summary
This PR modernizes the DuckEL repository by implementing a consistent naming convention and reorganizing the project structure to separate concerns. It addresses technical debt in the core engine and prepares the codebase for future scalability.

## Changes

### 🏗️ Structural Reorganization
- **`ui/`**: Created `ui/main.py` (moved from `app.py`) to encapsulate the Streamlit application.
- **`configs/`**: Created a centralized configuration directory for YAML pipelines and environment templates.
- **`scripts/`**: Moved utility scripts like `generate_data.py`.
- **`logs/`**: Created a directory for execution logs, protected by `.gitignore`.

### 🔧 Core Refactoring
- **Adapters (`duckel/adapters.py`)**:
    - Renamed internal utility methods to `_snake_case` (e.g., `_sanitize_identifier`).
    - Updated default attachment names (`pg_source_attachment`, `sf_source_attachment`) for clarity.
- **Configuration (`duckel/config.py`)**:
    - Fixed environment token resolution bug (`__ENV:VAR`) to correctly handle suffixes.
    - Standardized internal token parsing logic.
- **Models (`duckel/models.py`)**:
    - Enforced consistent field validators.

### 🖥️ UI Enhancements
- Updated `ui/main.py` to support the new directory structure.
- Standardized Session State keys (e.g., `app_started`).
- Standardized Component keys (e.g., `btn_run_pipeline`).
- Fixed `ModuleNotFoundError` by correctly adding project root to `sys.path`.

### 📚 Documentation & Cleanup
- Updated `README.md` with new run instructions (`streamlit run ui/main.py`).
- Updated `.gitignore` to exclude `*.log` and `logs/` directory.
- Removed tracking of `duckel.log`.

## Verification
- **Unit Tests**: All 75 tests passed.
    - Updated `test_evolution.py` for new schema validation logic.
    - Updated `test_config.py` for regex-based token resolution.
    - Updated `test_runner.py` for case-insensitive compression assertions.
- **Integration**: Validated using `pytest` against local test environment.

## Checklist
- [x] Structrual moves completed
- [x] Code refactoring completed
- [x] Tests updated and passing
- [x] Documentation updated
