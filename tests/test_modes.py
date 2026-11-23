"""
Every target against every write mode: the pair runs, or it is refused when the configuration loads.

Each target declares the modes it supports as data, `TargetAdapter.modes`. A configuration that
names a mode its target lacks never reaches the run.
"""

import pytest
from pydantic import ValidationError

from duckel.adapters import TARGETS, WriteMode, create_target_adapter
from duckel.config import load_config
from duckel.errors import ConfigError, ConfigErrorCategory
from duckel.models import PipelineConfig, TargetConfig
from tests.vectors import BATCH, COLUMNS, DATABASE_TARGETS, FILE_READERS, FILE_TARGETS, HELD, values

PAIRS = [(target, mode) for target in sorted(TARGETS) for mode in WriteMode]

# What each mode leaves behind, when a target holds HELD and a run brings BATCH.
LEFT_BY = {
    WriteMode.OVERWRITE: [(1, "b"), (3, "c")],
    WriteMode.APPEND: [(1, "a"), (1, "b"), (2, "z"), (3, "c")],
    WriteMode.UPSERT: [(1, "b"), (2, "z"), (3, "c")],
}
FILE_PAIRS = [
    pytest.param(target, mode, id=f"{target}-{mode.value}")
    for target in FILE_TARGETS
    for mode in sorted(TARGETS[target].modes)
]

# The statement each mode builds on a database target. No live write: the text is the test.
STATEMENT_OF = {
    WriteMode.OVERWRITE: "CREATE TABLE tgt.t AS",
    WriteMode.APPEND: "INSERT INTO tgt.t",
    WriteMode.UPSERT: "INSERT OR REPLACE INTO tgt.t",
}
DATABASE_PAIRS = [
    pytest.param(target, mode, id=f"{target}-{mode.value}")
    for target in DATABASE_TARGETS
    for mode in sorted(TARGETS[target].modes)
]

CSV_UPSERT_YAML = """
pipelines:
  orders_to_csv:
    source:
      type: parquet
      path: ./orders.parquet
    target:
      type: csv
      path: ./orders.csv
      mode: upsert
      unique_key: id
"""


def target_config(target: str, mode: WriteMode) -> dict:
    """A target configuration with every field a target may ask for."""
    return {
        "type": target,
        "path": f"./out.{target}",
        "conn": "host=localhost",
        "name": "tgt",
        "table": "t",
        "unique_key": "id",
        "mode": mode.value,
    }


def test_every_target_declares_the_modes_it_supports():
    for name, adapter in TARGETS.items():
        assert adapter.modes, f"{name} declares no mode"
        assert adapter.modes <= set(WriteMode), f"{name} declares a mode that does not exist"
        assert adapter.default_mode in adapter.modes, f"{name} defaults to a mode it lacks"


@pytest.mark.parametrize("target", sorted(TARGETS))
def test_a_target_with_no_mode_gets_the_default_it_declares(target):
    config = target_config(target, WriteMode.OVERWRITE)
    del config["mode"]

    assert TargetConfig(**config).mode == TARGETS[target].default_mode.value
    assert TARGETS[target](config).mode is TARGETS[target].default_mode


@pytest.mark.parametrize("target", FILE_TARGETS)
def test_append_on_a_file_target_is_refused(target):
    """COPY ... TO replaces the file, so a file target cannot keep the rows it holds."""
    with pytest.raises(ConfigError, match="append") as refused:
        TargetConfig(**target_config(target, WriteMode.APPEND))
    assert refused.value.hint == f"Use one of the modes a {target} target supports: overwrite."


@pytest.mark.parametrize("target,mode", PAIRS, ids=[f"{t}-{m.value}" for t, m in PAIRS])
def test_each_mode_runs_or_is_refused(target, mode):
    config = target_config(target, mode)
    if mode in TARGETS[target].modes:
        loaded = TargetConfig(**config)
        assert TARGETS[target](loaded.model_dump()).build_write_sql("source", COLUMNS)
    else:
        with pytest.raises(ConfigError, match=mode.value) as refused:
            TargetConfig(**config)
        assert refused.value.category is ConfigErrorCategory.UNSUPPORTED_MODE
        hint = refused.value.hint
        assert all(m.value in hint for m in TARGETS[target].modes), "the hint names the modes"


@pytest.mark.parametrize("target,mode", FILE_PAIRS)
def test_a_file_target_leaves_what_its_mode_says(con, tmp_path, target, mode):
    path = (tmp_path / f"t.{target}").as_posix()
    held = {"type": target, "path": path, "mode": WriteMode.OVERWRITE.value}
    con.execute(create_target_adapter(held).build_write_sql(values(HELD), COLUMNS))

    batch = create_target_adapter({**held, "mode": mode.value})
    con.execute(batch.build_write_sql(values(BATCH), COLUMNS))

    left = con.execute(f"SELECT id, v FROM {FILE_READERS[target]}('{path}') ORDER BY id, v")
    assert left.fetchall() == LEFT_BY[mode]


@pytest.mark.parametrize("target,mode", DATABASE_PAIRS)
def test_a_database_target_builds_the_statement_of_its_mode(target, mode):
    loaded = TargetConfig(**target_config(target, mode))
    sql = TARGETS[target](loaded.model_dump()).build_write_sql("source", COLUMNS)
    assert STATEMENT_OF[mode] in sql
    assert [text for text in STATEMENT_OF.values() if text in sql] == [STATEMENT_OF[mode]]


def test_the_refusal_names_the_target_the_mode_and_what_to_use():
    with pytest.raises(ConfigError) as refused:
        TargetConfig(**target_config("snowflake", WriteMode.UPSERT))
    error = refused.value
    assert error.category is ConfigErrorCategory.UNSUPPORTED_MODE
    assert "snowflake" in error.message and "upsert" in error.message
    assert error.hint == "Use one of the modes a snowflake target supports: append, overwrite."
    assert str(error) == f"[unsupported_mode] {error.message} {error.hint}"


def test_a_pipeline_is_refused_when_its_target_is():
    with pytest.raises(ConfigError, match="upsert"):
        PipelineConfig(
            source={"type": "parquet", "path": "./in.parquet"},
            target=target_config("parquet", WriteMode.UPSERT),
        )


def test_a_yaml_configuration_is_refused_when_it_loads(tmp_path):
    config_file = tmp_path / "pipelines.yml"
    config_file.write_text(CSV_UPSERT_YAML, encoding="utf-8")

    with pytest.raises(ConfigError, match="orders_to_csv") as refused:
        load_config(str(config_file))
    assert refused.value.category is ConfigErrorCategory.UNSUPPORTED_MODE
    assert "overwrite" in refused.value.hint


def test_a_mode_that_does_not_exist_is_still_refused_by_the_model():
    with pytest.raises(ValidationError):
        TargetConfig(type="parquet", path="./out.parquet", mode="merge")
