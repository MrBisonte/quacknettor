# ADR-0001: The design rules of arch-standards are adopted, and its scan runs from a checkout

| | |
|---|---|
| Status | proposed |
| Date | 2026-11-23 |
| Deciders | Alexandre Monteiro |
| Supersedes | none |
| Superseded by | |

## Shape

```
 configuration                    YAML or dict
   │  type in SOURCES, TARGETS    one registry each, duckel/adapters.py            rule 4
   │  mode in target.modes        refused when it loads, with a ConfigError        rule 6
   ▼
 PipelineRunner(config, clock)    the clock is handed in                           rule 10
   │  run()                       opens DuckDBEngine, defaults from settings.py    rule 1
   │  run_on(con)                 the same stages on a connection the caller opened
   ▼
 TargetAdapter
   │  build_write_sql(relation, columns)             every statement names its columns   rule 9
   │  write(con, sql) -> WriteReport(rows, bytes)    counted by the write itself         rule 13
   ▼
 Parquet, CSV: rows and bytes          Postgres, Snowflake: rows, no bytes
```

- Design follows `arch-standards/code/design/SPEC.md`, which holds 13 rules.
- The scan of arch-standards, `scripts/design_check.py`, decides rules 1, 2, 9 and 10. It is not
  part of this repository. Run from a checkout of arch-standards, it exits 0 on 2026-11-23.
- The remote check does not run the scan. It runs the unit suite, which checks rules 4, 6, 9,
  10 and 13.
- 5 of the 13 rules have a check in this repository: 4, 6, 9, 10 and 13. Rules 1 and 2 are
  decided by the scan alone.
- The deviations table at the end lists what does not conform yet.

## Context

arch-standards restates the five SOLID principles as 13 rules for code that moves data. This
project is the first to adopt them. The rules were read against `d382125`, and seven breaches
were found in the `duckel` package.

| Found at `d382125` | Rule | Changed to |
|---|---|---|
| `mode: upsert` validated for every target. Snowflake built a plain INSERT. Parquet and CSV replaced the file | 6 | Each target declares its modes. Any other mode is refused when the configuration loads |
| `mode: append` on a Parquet or CSV target replaced the file, and `append` was the default mode | 6 | A file target declares `overwrite` only, and gets it when no mode is set |
| The source type `duckdb` validated and had no adapter | 4, 6 | The validated list is the registry |
| Three inserts aligned their values by position | 9 | Every statement names its columns |
| The thread and memory defaults stood in three places, the compression default in two | 1 | One home, `duckel/settings.py` |
| The runner read the wall clock in place | 10 | The runner is handed a clock |
| A write returned nothing, and the reported count came from another scan | 13 | A write returns the rows and the bytes it moved |

Before the fixes the scan reported 9 findings on 7 lines under the declaration below: 5 for
rule 1, 3 for rule 9 and 1 for rule 10.

## Options considered

How this repository uses the scan and the proof helpers of arch-standards.

| Option | Summary | Rejected because |
|---|---|---|
| Copy the scan, the helpers and their tests into `scripts/` | The remote check runs the scan with the unit suite | arch-standards is a private repository and this one is public. A copy publishes its files |
| Depend on arch-standards as a package | Nothing is copied | arch-standards is a private repository and publishes no package |
| Run the scan from a checkout of arch-standards | Nothing of arch-standards is part of this repository | chosen |

## Decision

1. Design follows `arch-standards/code/design/SPEC.md`. The open sets and their registries:

| Open set | Registry | A new member is |
|---|---|---|
| Sources | `SOURCES` in `duckel/adapters.py` | One row plus one `SourceAdapter` class |
| Targets | `TARGETS` in `duckel/adapters.py` | One row plus one `TargetAdapter` class |

Write modes and what a write counts are closed sets, `WriteMode` and `Counted`. Each target
declares three things as data: the modes it supports, the mode it gets when a configuration
names none, and what its write counts.

| Target | `modes` | `default_mode` | `counts` |
|---|---|---|---|
| Parquet, CSV | overwrite | overwrite | rows, bytes |
| Postgres | overwrite, append, upsert | append | rows |
| Snowflake | overwrite, append | append | rows |

2. Nothing of arch-standards is copied into this repository. The scan and the proof helpers
   stay in arch-standards. The scan runs from a checkout of it, at the root of this
   repository, and `ARCH_STANDARDS` is the path of that checkout. It exits 0 with
   arch-standards at commit `e9162d4`, read on 2026-11-23.

```bash
python "$ARCH_STANDARDS/scripts/design_check.py" .
```

3. `.design.yaml` at the root declares what the scan reads.

| Key | Value | Reason |
|---|---|---|
| `sources` | `duckel/**/*.py` | The package that moves data |
| `settings` | `duckel/settings.py` | The one home of threads, memory limit and compression |
| `semantic` | `duckel/runner.py` | The runner decides what lands, and names no engine |
| `edge` | `duckel/engine.py`, `duckel/clock.py` | The engine opens the connection. The clock module reads the wall clock |
| `exceptions` | Two, on `duckel/engine.py`, rule 1 | A usage example in a docstring holds `threads=8` and `memory_limit="4GB"` |

4. The remote check does not run the scan: it has no checkout of arch-standards. No workflow
   file changes. One test needs a proof helper of arch-standards,
   `test_the_size_of_a_file_is_read_without_its_content` in `tests/test_write.py`. It finds
   the checkout through `ARCH_STANDARDS`, and reports as skipped with that reason where the
   variable is not set, as on the remote check.

5. The standard asks for one line in the project's `CLAUDE.md`. This project has none, so the
   line lives in `README.md`, under "Design rules". Decided on 2026-11-23.

6. A write runs in a transaction, and the bytes are read after the commit. When the engine
   cannot read the size of a written file, the write returns no byte count and logs a warning.
   The rows are committed by then, so the run goes on.

7. Where each rule is checked.

| Rule | Check | Where |
|---|---|---|
| 1 | The scan | `.design.yaml`, `settings` |
| 2 | The scan | `.design.yaml`, `semantic` |
| 4 | The validated names equal the keys of the registry | `tests/test_registry.py` |
| 6 | Every target against every mode runs, or is refused when it loads | `tests/test_modes.py` |
| 9 | The scan, and each statement names its columns | `tests/test_write.py` |
| 10 | The scan, and the runner driven through a connection and a clock | `tests/test_runner_seams.py` |
| 13 | A write returns its counts, and a second read finds the same | `tests/test_write.py` |
| 3, 5, 7, 8, 11, 12 | None | See the deviations table |

The scan runs from a checkout of arch-standards, outside the remote check. The tests named in
the table run in the remote check.

## Consequences

- Positive: a configuration that names a missing source, target or mode stops when it loads,
  with the modes that target does support in the message.
- Positive: every test of this change runs on DuckDB in process and on local files. None opens
  `DuckDBEngine`, so none needs an extension or the network.
- Negative: no statement was run on Postgres or Snowflake. The tests of a database target run
  the adapter's own statement on a DuckDB attachment.
- Negative: a configuration that sets `mode: append` on a Parquet or CSV target no longer
  loads. It has to say `overwrite`, which is what the write did.
- Negative: an insert names its columns, so the engine no longer refuses a target that holds a
  column the source lacks. That column is filled with NULL.
- Negative: three signatures change. `TargetAdapter.build_write_sql` and
  `build_target_write_sql` take the columns, and `PipelineRunner` takes a clock.
- Negative: a target adapter written outside this repository has to declare `modes`,
  `default_mode` and `counts`, and implement `bytes_written`.
- Negative: the remote check does not run the scan. A breach of rule 1 or 2 shows only when
  the scan is run from a checkout of arch-standards.

## Deviations

| # | Rule | What does not conform | Reason | State |
|---|---|---|---|---|
| 1 | 13 | A Postgres or Snowflake write returns rows and no bytes | The engine reports a row count for an insert and no byte count | Declared in `counts`. Each run logs a warning |
| 2 | 13 | The `rows` a run reports and the watermark it saves still come from their own scans | Taking `rows` from the write changes the result of `run` and removes the `compute_counts` option | Open decision |
| 3 | 6 | `upsert` on Postgres was never run on Postgres | No database was reachable for this change. The statement is DuckDB's `INSERT OR REPLACE`, proved on a DuckDB table with a primary key | Open. One run on a Postgres table settles it |
| 4 | 6 | `upsert` with no `unique_key` loads, then stops in the run. The key never reaches the statement | The check sits in `build_write_sql`, as it did at `d382125` | Open |
| 5 | 4 | `duckel/models.py` lists type names in four validators, for the `path` and `conn` a type requires | The lists were there at `d382125`. A new member needs its name added there too | Open |
| 6 | 10 | `ui/` and `scripts/` are outside the scan. `ui/main.py` holds 15 lines that open a connection or read the clock, and offers every mode for every target | Screen code and tools were not part of this adoption | Open |
| 7 | 3, 5, 7, 8, 11, 12 | No hash of the output before and after a tuning change, no type table, no single suite for every target, no list of allowed differences, no written budget, and no proof that the incremental filter reaches the source | Not part of this adoption. Rule 12 is proved for one read, the size of a written file, by a test that is skipped where `ARCH_STANDARDS` is not set | Open |
| 8 | 1, 2 | No check in this repository decides them. The scan does, and it runs only from a checkout of arch-standards | arch-standards is a private repository, so its scan is not copied here | Open |
