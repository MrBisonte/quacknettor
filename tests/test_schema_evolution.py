"""
`schema_evolution` on every target, when the source and the target hold different columns.

The target holds (0, 'held') in the columns (id, v). The source has gained `extra`, and the
batch is (1, 'b', 9). Every test runs on DuckDB in process. For a database target a DuckDB
attachment stands in under the alias the adapter writes to.
"""

import pytest

from duckel.adapters import TARGETS, AdapterError, create_target_adapter
from duckel.models import PipelineConfig, TargetConfig
from duckel.runner import PipelineRunner
from tests.vectors import DATABASE_TARGETS, FILE_READERS, FILE_TARGETS

ALIAS = "tgt"
EVERY_TARGET = sorted(TARGETS)
MISMATCH = "Schema mismatch"


@pytest.fixture
def source(con) -> str:
    """The batch (1, 'b', 9): the source has gained the column extra."""
    con.execute("CREATE TABLE source AS SELECT 1 AS id, 'b' AS v, 9 AS extra")
    return "source"


@pytest.fixture
def narrow(con) -> str:
    """The batch (1, 'b'): the source holds id and v only."""
    con.execute("CREATE TABLE narrow AS SELECT 1 AS id, 'b' AS v")
    return "narrow"


@pytest.fixture
def held(con, tmp_path):
    """Build a target of one kind that holds one row. Returns its configuration and its reader."""

    def build(kind: str, columns: str = "id INTEGER, v VARCHAR", row: str = "(0, 'held')"):
        names = [column.split()[0] for column in columns.split(", ")]
        if kind in FILE_TARGETS:
            path = (tmp_path / f"t.{kind}").as_posix()
            first = create_target_adapter({"type": kind, "path": path})
            rows = f"(VALUES {row}) AS rows({', '.join(names)})"
            con.execute(first.build_write_sql(rows, names))
            return {"type": kind, "path": path}, f"{FILE_READERS[kind]}('{path}')"
        con.execute(f"ATTACH ':memory:' AS {ALIAS}")
        con.execute(f"CREATE TABLE {ALIAS}.t ({columns})")
        con.execute(f"INSERT INTO {ALIAS}.t ({', '.join(names)}) VALUES {row}")
        config = {"type": kind, "conn": "host=localhost", "name": ALIAS, "table": "t"}
        return {**config, "mode": "append"}, f"{ALIAS}.t"

    return build


def load(con, config: dict, relation: str):
    """What a run does with one batch: check the schema, then write the columns it returns."""
    adapter = create_target_adapter(TargetConfig(**config).model_dump())
    columns = adapter.sync_schema(con, relation)
    return adapter.write(con, adapter.build_write_sql(relation, columns))


def rows_of(con, reader: str) -> list:
    return con.execute(f"SELECT * FROM {reader} ORDER BY id").fetchall()


def columns_of(con, reader: str) -> list:
    return [row[0] for row in con.execute(f"DESCRIBE SELECT * FROM {reader}").fetchall()]


# What each kind of target holds after the batch, when the setting lets the run continue.
# A database append keeps the held row. A file is replaced.
def left(kind: str, batch: tuple, held_row: tuple) -> list:
    return [batch] if kind in FILE_TARGETS else [held_row, batch]


@pytest.mark.parametrize("setting", [{"schema_evolution": "ignore"}, {}], ids=["ignore", "unset"])
@pytest.mark.parametrize("kind", EVERY_TARGET)
def test_ignore_warns_and_continues(con, source, held, caplog, kind, setting):
    config, reader = held(kind)

    with caplog.at_level("WARNING", logger="duckel"):
        load(con, {**config, **setting}, source)

    assert MISMATCH in caplog.text and "extra" in caplog.text, "the warning names the column"
    if kind in FILE_TARGETS:
        assert rows_of(con, reader) == [(1, "b", 9)], "the file is replaced, with the new column"
    else:
        assert rows_of(con, reader) == [(0, "held"), (1, "b")], "extra is not loaded"
        assert columns_of(con, reader) == ["id", "v"]


@pytest.mark.parametrize("kind", EVERY_TARGET)
def test_fail_stops_before_any_write(con, source, held, kind):
    config, reader = held(kind)

    with pytest.raises(AdapterError, match=MISMATCH) as stopped:
        load(con, {**config, "schema_evolution": "fail"}, source)

    assert "extra" in str(stopped.value)
    assert rows_of(con, reader) == [(0, "held")], "the target is as it was"


@pytest.mark.parametrize("kind", EVERY_TARGET)
def test_evolve_takes_the_new_column(con, source, held, kind):
    config, reader = held(kind)

    load(con, {**config, "schema_evolution": "evolve"}, source)

    assert columns_of(con, reader) == ["id", "v", "extra"]
    assert rows_of(con, reader) == left(kind, (1, "b", 9), (0, "held", None))


@pytest.mark.parametrize("kind", EVERY_TARGET)
def test_ignore_warns_about_a_target_column_the_source_lacks(con, narrow, held, caplog, kind):
    config, reader = held(kind, "id INTEGER, v VARCHAR, amount INTEGER", "(0, 'held', 5)")

    with caplog.at_level("WARNING", logger="duckel"):
        load(con, config, narrow)

    assert MISMATCH in caplog.text and "amount" in caplog.text
    if kind in FILE_TARGETS:
        assert rows_of(con, reader) == [(1, "b")], "the file is replaced, without amount"
    else:
        assert rows_of(con, reader) == [(0, "held", 5), (1, "b", None)], "amount is NULL"


@pytest.mark.parametrize("kind", EVERY_TARGET)
def test_fail_stops_on_a_target_column_the_source_lacks(con, narrow, held, kind):
    config, reader = held(kind, "id INTEGER, v VARCHAR, amount INTEGER", "(0, 'held', 5)")

    with pytest.raises(AdapterError, match="amount"):
        load(con, {**config, "schema_evolution": "fail"}, narrow)

    assert rows_of(con, reader) == [(0, "held", 5)]


@pytest.mark.parametrize("setting", ["ignore", "fail", "evolve"])
@pytest.mark.parametrize("kind", EVERY_TARGET)
def test_the_same_columns_load_with_no_warning(con, narrow, held, caplog, kind, setting):
    config, reader = held(kind)

    with caplog.at_level("WARNING", logger="duckel"):
        load(con, {**config, "schema_evolution": setting}, narrow)

    assert MISMATCH not in caplog.text
    assert rows_of(con, reader) == left(kind, (1, "b"), (0, "held"))


@pytest.mark.parametrize("kind", DATABASE_TARGETS)
def test_column_names_are_compared_without_case(con, narrow, held, caplog, kind):
    config, reader = held(kind, '"ID" INTEGER, "V" VARCHAR')

    with caplog.at_level("WARNING", logger="duckel"):
        load(con, {**config, "schema_evolution": "fail"}, narrow)

    assert MISMATCH not in caplog.text
    assert rows_of(con, reader) == [(0, "held"), (1, "b")]


@pytest.mark.parametrize("kind", FILE_TARGETS)
def test_a_file_that_does_not_exist_yet_is_written(con, source, tmp_path, caplog, kind):
    path = (tmp_path / f"new.{kind}").as_posix()

    with caplog.at_level("WARNING", logger="duckel"):
        load(con, {"type": kind, "path": path, "schema_evolution": "fail"}, source)

    assert MISMATCH not in caplog.text
    assert rows_of(con, f"{FILE_READERS[kind]}('{path}')") == [(1, "b", 9)]


@pytest.mark.parametrize("kind", DATABASE_TARGETS)
def test_an_append_to_a_table_that_does_not_exist_is_refused(con, source, kind):
    con.execute(f"ATTACH ':memory:' AS {ALIAS}")
    config = {"type": kind, "conn": "host=localhost", "name": ALIAS, "table": "t", "mode": "append"}

    with pytest.raises(AdapterError, match="not found") as refused:
        load(con, config, source)
    assert "overwrite" in str(refused.value), "the message names the mode that creates the table"


@pytest.mark.parametrize("kind", DATABASE_TARGETS)
def test_an_overwrite_recreates_the_table_with_the_columns_of_the_source(con, source, held, kind):
    config, reader = held(kind)

    load(con, {**config, "mode": "overwrite"}, source)

    assert columns_of(con, reader) == ["id", "v", "extra"]
    assert rows_of(con, reader) == [(1, "b", 9)]


# The runner: the setting of the target is read, and an option given for one run wins over it.


@pytest.fixture
def pipeline(con, tmp_path, held):
    """A Parquet source that has gained `extra`, and a Parquet target that holds (id, v)."""
    source_file = (tmp_path / "source.parquet").as_posix()
    con.execute(f"COPY (SELECT 1 AS id, 'b' AS v, 9 AS extra) TO '{source_file}' (FORMAT parquet)")
    target, reader = held("parquet")

    def build(setting: str) -> PipelineConfig:
        return PipelineConfig(
            source={"type": "parquet", "path": source_file},
            target={**target, "schema_evolution": setting},
        )

    return build, reader


def test_a_run_reads_the_setting_of_its_target(con, pipeline):
    build, reader = pipeline

    with pytest.raises(AdapterError, match=MISMATCH):
        PipelineRunner(build("fail")).run_on(con)

    assert rows_of(con, reader) == [(0, "held")]


def test_an_option_given_for_one_run_wins_over_the_target(con, pipeline):
    build, reader = pipeline

    PipelineRunner(build("fail"), {"schema_evolution": "ignore"}).run_on(con)

    assert rows_of(con, reader) == [(1, "b", 9)]
