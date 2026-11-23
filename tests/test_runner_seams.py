"""
The runner, driven through what it is handed: a connection and a clock.

DuckDB in process is the engine, with no extension loaded, and a function that returns a fixed
time is the clock. Nothing here opens `DuckDBEngine`, sleeps or reads the wall clock.
"""

import json

import pytest

from duckel.models import PipelineConfig
from duckel.runner import PipelineExecutionError, PipelineRunner

FIXED_TIME = 1_763_800_000.0  # 2025-11-22T08:26:40Z, any fixed instant


@pytest.fixture
def source_file(con, tmp_path) -> str:
    """Five rows in one Parquet file: id, name, updated_at."""
    path = (tmp_path / "source.parquet").as_posix()
    con.execute(
        f"COPY (SELECT i AS id, 'n' || i AS name, i * 100 AS updated_at FROM range(1, 6) t(i)) "
        f"TO '{path}' (FORMAT parquet)"
    )
    return path


def pipeline(source_file: str, target_file: str, **source) -> PipelineConfig:
    return PipelineConfig(
        source={"type": "parquet", "path": source_file, **source},
        target={"type": "parquet", "path": target_file, "mode": "overwrite"},
    )


def test_a_run_reports_the_rows_and_the_bytes_the_write_moved(con, source_file, tmp_path):
    target_file = tmp_path / "target.parquet"
    runner = PipelineRunner(pipeline(source_file, target_file.as_posix()))

    result = runner.run_on(con)

    assert result["written"] == {"rows": 5, "bytes": target_file.stat().st_size}
    assert result["rows"] == 5
    landed = con.execute(f"SELECT count(*) FROM read_parquet('{target_file.as_posix()}')")
    assert landed.fetchone()[0] == result["written"]["rows"]


def test_a_run_reports_what_the_write_moved_with_the_count_stage_off(con, source_file, tmp_path):
    target_file = tmp_path / "target.parquet"
    config = pipeline(source_file, target_file.as_posix())
    runner = PipelineRunner(config, {"compute_counts": False, "sample_data": False})

    result = runner.run_on(con)

    assert result["rows"] == "N/A"
    assert result["written"] == {"rows": 5, "bytes": target_file.stat().st_size}


def test_the_write_names_the_columns_of_the_source(con, source_file, tmp_path):
    runner = PipelineRunner(pipeline(source_file, (tmp_path / "target.parquet").as_posix()))

    result = runner.run_on(con)

    assert 'SELECT "id", "name", "updated_at" FROM' in result["write_sql"]


def test_the_state_file_is_stamped_by_the_clock_the_runner_is_handed(con, source_file, tmp_path):
    state_path = tmp_path / "state.json"
    target_file = (tmp_path / "target.parquet").as_posix()
    config = pipeline(source_file, target_file, incremental_key="updated_at")
    runner = PipelineRunner(config, pipeline_name="orders", clock=lambda: FIXED_TIME)
    runner._get_state_path = lambda: state_path

    runner.run_on(con)

    assert json.loads(state_path.read_text()) == {
        "orders": {"watermark": 500, "last_run": FIXED_TIME}
    }


def test_a_write_that_fails_is_a_pipeline_execution_error(con, source_file, tmp_path):
    unwritable = (tmp_path / "no_such_folder" / "target.parquet").as_posix()
    runner = PipelineRunner(pipeline(source_file, unwritable))

    with pytest.raises(PipelineExecutionError, match="Write operation failed"):
        runner.run_on(con)


def test_a_source_that_is_missing_stops_the_run_before_any_write(con, tmp_path):
    target_file = tmp_path / "target.parquet"
    missing = (tmp_path / "no_such_source.parquet").as_posix()
    runner = PipelineRunner(pipeline(missing, target_file.as_posix()))

    with pytest.raises(Exception, match="no_such_source"):
        runner.run_on(con)
    assert not target_file.exists()
