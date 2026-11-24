"""
The one home of the tuning values.

A tuning value changes how fast a run is, never what it writes. Each default below reaches the
unit that uses it as data, and no other module spells one out. The design scan of arch-standards
fails on a tuning key given a literal value elsewhere in the package, in the forms a text scan
reads. That scan runs from a checkout of arch-standards, see docs/adr/ADR-0001-design-rules.md.
"""

# Threads DuckDB runs a pipeline on.
DEFAULT_THREADS = 4

# Memory DuckDB may use before it spills to disk.
DEFAULT_MEMORY_LIMIT = "2GB"

# Codec of a Parquet target.
DEFAULT_COMPRESSION = "zstd"
