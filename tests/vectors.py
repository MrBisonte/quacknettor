"""
Shared tuples for the write tests, and how each file target is read back.

Every test that uses them runs on DuckDB in process and on local files.
"""

from duckel.adapters import TARGETS, DatabaseTargetAdapter, FileTargetAdapter

# The columns of every relation below, in order.
COLUMNS = ("id", "v")

# What a target holds before a run, and the batch the run brings.
HELD = [(1, "a"), (2, "z")]
BATCH = [(1, "b"), (3, "c")]

# The function that reads each file target back.
FILE_READERS = {"csv": "read_csv_auto", "parquet": "read_parquet"}

FILE_TARGETS = sorted(n for n, adapter in TARGETS.items() if issubclass(adapter, FileTargetAdapter))
DATABASE_TARGETS = sorted(
    n for n, adapter in TARGETS.items() if issubclass(adapter, DatabaseTargetAdapter)
)


def values(rows: list) -> str:
    """Rows as a relation with the columns id and v."""
    return f"(VALUES {', '.join(str(row) for row in rows)}) AS rows(id, v)"
