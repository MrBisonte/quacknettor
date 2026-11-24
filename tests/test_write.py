"""
A write names the columns it fills, and returns the rows and the bytes it moved.

Every test runs on DuckDB in process. For a database target a DuckDB attachment stands in under
the alias the adapter writes to: the statement is the adapter's own, the engine is not Postgres
and not Snowflake.
"""

import os
import runpy
from pathlib import Path

import pytest

from duckel.adapters import (
    TARGETS,
    AdapterError,
    Counted,
    WriteMode,
    WriteReport,
    create_target_adapter,
    file_size_sql,
    quote_identifier,
)
from duckel.models import TargetConfig
from tests.vectors import COLUMNS, DATABASE_TARGETS, FILE_READERS, FILE_TARGETS

ROWS = [(1, "a"), (2, "b"), (3, None)]
ALIAS = "tgt"
DECLARED_PAIRS = [
    pytest.param(target, mode, id=f"{target}-{mode.value}")
    for target in sorted(TARGETS)
    for mode in sorted(TARGETS[target].modes)
]
DATABASE_PAIRS = [
    pytest.param(target, mode, id=f"{target}-{mode.value}")
    for target in DATABASE_TARGETS
    for mode in sorted(TARGETS[target].modes)
]

# The proof helpers of arch-standards are not part of this repository. ARCH_STANDARDS is the path
# of a checkout of it. A test that needs a helper is skipped, with its reason, where the helper
# cannot be reached.
ARCH_STANDARDS = os.environ.get("ARCH_STANDARDS")


def target(kind: str, mode: WriteMode, path: str = "./out") -> dict:
    """A target configuration with every field a target may ask for."""
    return {
        "type": kind,
        "path": path,
        "conn": "host=localhost",
        "name": ALIAS,
        "table": "t",
        "unique_key": "id",
        "mode": mode.value,
    }


@pytest.fixture
def source(con) -> str:
    """Three rows, one with a NULL, as a table named source."""
    con.execute("CREATE TABLE source (id INTEGER, v VARCHAR)")
    con.executemany("INSERT INTO source (id, v) VALUES (?, ?)", ROWS)
    return "source"


@pytest.fixture
def database(con) -> str:
    """A DuckDB attachment under the alias a database target writes to."""
    con.execute(f"ATTACH ':memory:' AS {ALIAS}")
    con.execute(f"CREATE TABLE {ALIAS}.t (id INTEGER PRIMARY KEY, v VARCHAR)")
    return f"{ALIAS}.t"


@pytest.fixture
def design_proof() -> dict:
    """The names scripts/design_proof.py of arch-standards defines, or a skip that says why."""
    if not ARCH_STANDARDS:
        pytest.skip("ARCH_STANDARDS not set: the proof helper of arch-standards cannot be reached")
    helper = Path(ARCH_STANDARDS) / "scripts" / "design_proof.py"
    if not helper.is_file():
        pytest.skip(f"ARCH_STANDARDS holds no scripts/design_proof.py: {ARCH_STANDARDS}")
    return runpy.run_path(str(helper))


def rows_in_key_order(con, relation: str) -> list:
    """Every row of a relation, read in the order of its key."""
    return con.execute(f"SELECT * FROM {relation} ORDER BY id").fetchall()


# Rule 9: a write names the columns it fills.


@pytest.mark.parametrize("kind,mode", DECLARED_PAIRS)
def test_a_statement_names_the_columns_it_fills(kind, mode):
    sql = create_target_adapter(target(kind, mode)).build_write_sql("source", COLUMNS)
    assert 'SELECT "id", "v" FROM source' in sql
    assert "*" not in sql


@pytest.mark.parametrize("kind,mode", DATABASE_PAIRS)
def test_an_insert_lists_its_columns(kind, mode):
    sql = create_target_adapter(target(kind, mode)).build_write_sql("source", COLUMNS)
    if mode is not WriteMode.OVERWRITE:
        assert f'INTO {ALIAS}.t ("id", "v") SELECT' in sql


@pytest.mark.parametrize("kind", sorted(TARGETS))
def test_a_write_with_no_column_is_refused(kind):
    adapter = create_target_adapter(target(kind, WriteMode.OVERWRITE))
    with pytest.raises(AdapterError, match="no column"):
        adapter.build_write_sql("source", ())


def test_a_column_name_is_quoted_and_its_quotes_are_doubled():
    assert quote_identifier("id") == '"id"'
    assert quote_identifier('say "hi"') == '"say ""hi"""'


@pytest.mark.parametrize("kind", DATABASE_TARGETS)
def test_a_database_target_with_no_name_writes_to_its_default_alias(kind):
    """A loaded configuration holds `name: None`, and the default alias still applies."""
    loaded = TargetConfig(type=kind, conn="host=localhost", table="t").model_dump()
    adapter = create_target_adapter(loaded)

    assert loaded["name"] is None
    assert adapter.full_table() == f"{TARGETS[kind].default_alias}.t"


@pytest.mark.parametrize("kind", DATABASE_TARGETS)
def test_an_insert_aligns_by_name_not_by_position(con, source, kind):
    """The target lists its columns in another order than the source."""
    con.execute(f"ATTACH ':memory:' AS {ALIAS}")
    con.execute(f"CREATE TABLE {ALIAS}.t (v VARCHAR, id INTEGER)")
    adapter = create_target_adapter(target(kind, WriteMode.APPEND))

    adapter.write(con, adapter.build_write_sql(source, COLUMNS))

    assert con.execute(f"SELECT id, v FROM {ALIAS}.t ORDER BY id").fetchall() == ROWS


# Rule 13: a write returns the rows and the bytes it moved, counted by the write itself.


def test_every_target_declares_what_it_counts():
    for name, adapter in TARGETS.items():
        assert Counted.ROWS in adapter.counts, f"{name} does not count rows"
        assert adapter.counts <= set(Counted), f"{name} declares a count that does not exist"


@pytest.mark.parametrize("kind", FILE_TARGETS)
def test_a_file_write_returns_what_it_moved(con, source, tmp_path, kind):
    path = tmp_path / f"out.{kind}"
    adapter = create_target_adapter(target(kind, WriteMode.OVERWRITE, path.as_posix()))

    report = adapter.write(con, adapter.build_write_sql(source, COLUMNS))

    assert report == WriteReport(rows=len(ROWS), bytes=path.stat().st_size)
    assert adapter.counts == {Counted.ROWS, Counted.BYTES}
    landed = f"{FILE_READERS[kind]}('{path.as_posix()}')"
    assert con.execute(f"SELECT count(*) FROM {landed}").fetchone()[0] == report.rows
    assert rows_in_key_order(con, landed) == rows_in_key_order(con, source)


@pytest.mark.parametrize("kind,mode", DATABASE_PAIRS)
def test_a_database_write_returns_its_rows_and_declares_no_bytes(con, source, database, kind, mode):
    adapter = create_target_adapter(target(kind, mode))

    report = adapter.write(con, adapter.build_write_sql(source, COLUMNS))

    assert report == WriteReport(rows=len(ROWS), bytes=None)
    assert adapter.counts == {Counted.ROWS}
    assert con.execute(f"SELECT count(*) FROM {database}").fetchone()[0] == report.rows
    assert rows_in_key_order(con, database) == rows_in_key_order(con, source)


def test_a_statement_that_returns_no_count_is_an_error(con):
    adapter = create_target_adapter(target("parquet", WriteMode.OVERWRITE))
    with pytest.raises(AdapterError, match="no row count"):
        adapter.write(con, "SELECT 'not a count'")


@pytest.mark.parametrize("kind", DATABASE_TARGETS)
def test_a_write_that_fails_leaves_the_target_as_it_was(con, source, database, kind):
    """The second statement fails, and the transaction takes the first one back with it."""
    con.execute(f"INSERT INTO {database} (id, v) VALUES (7, 'held')")
    adapter = create_target_adapter(target(kind, WriteMode.APPEND))
    failing = f"DELETE FROM {database}; INSERT INTO {database} (id) SELECT v FROM {source};"

    with pytest.raises(Exception, match="Could not convert"):
        adapter.write(con, failing)

    assert con.execute(f"SELECT id, v FROM {database}").fetchall() == [(7, "held")]


@pytest.mark.parametrize("kind", FILE_TARGETS)
def test_a_size_that_cannot_be_read_is_a_loud_skip(con, tmp_path, caplog, kind):
    """The rows are committed by the time the size is read, so the run goes on with no count."""
    missing = (tmp_path / f"never_written.{kind}").as_posix()
    adapter = create_target_adapter(target(kind, WriteMode.OVERWRITE, missing))

    with caplog.at_level("WARNING", logger="duckel"):
        assert adapter.bytes_written(con) is None
    assert missing in caplog.text


def test_the_size_of_a_file_is_read_without_its_content(con, source, tmp_path, design_proof):
    """Rule 12 on the one read the write adds: the engine is asked for `size` and no other column."""
    path = (tmp_path / "out.parquet").as_posix()
    con.execute(f"COPY {source} TO '{path}' (FORMAT parquet)")
    design_proof["assert_pushed"](con, file_size_sql(path), filters=[], columns=["size"])
