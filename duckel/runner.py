"""
Pipeline execution runner with comprehensive error handling.
"""

import json
import time
from pathlib import Path
from typing import Any, Optional

from duckdb import DuckDBPyConnection

from .adapters import (
    AdapterError,
    Counted,
    SourceAdapter,
    TargetAdapter,
    WriteReport,
    create_source_adapter,
    create_target_adapter,
)
from .clock import Clock, system_clock
from .engine import DuckDBEngine
from .logger import logger
from .models import PipelineConfig


class PipelineExecutionError(Exception):
    """Raised when pipeline execution fails."""

    pass


class PipelineRunner:
    """
    Orchestrates pipeline execution with proper error handling.
    """

    def __init__(
        self,
        pipeline_config: PipelineConfig,
        overrides: Optional[dict[str, Any]] = None,
        pipeline_name: str = "default",
        progress_callback: Optional[callable] = None,
        clock: Clock = system_clock,
    ):
        """
        Initialize pipeline runner.

        Args:
            pipeline_config: Validated PipelineConfig instance
            overrides: Optional dictionary of runtime option overrides
            pipeline_name: Name of the pipeline for state persistence
            progress_callback: Optional function(percent: int, message: str) called during execution
            clock: Returns the time a saved watermark is stamped with, in seconds since the epoch
        """
        self.config = pipeline_config
        self.options = pipeline_config.get_options(overrides)
        self.pipeline_name = pipeline_name
        self.progress_callback = progress_callback
        self.clock = clock
        self.metrics: dict[str, float] = {}

    def _report_progress(self, percent: int, message: str):
        """Invoke progress callback if configured."""
        if self.progress_callback:
            self.progress_callback(percent, message)

    def _get_state_path(self) -> Path:
        """Get path to state file."""
        # Simple local state file. In production this might be S3/DB.
        return Path(".duckel_state.json")

    def _get_watermark(self) -> Any:
        """Get last processed watermark for this pipeline."""
        try:
            path = self._get_state_path()
            if path.exists():
                with open(path) as f:
                    state = json.load(f)
                    val = state.get(self.pipeline_name, {}).get("watermark")
                    if val is not None:
                        logger.info(f"Found existing watermark: {val}")
                    return val
        except Exception as e:
            logger.warning(f"Failed to load state: {e}")
        return None

    def _save_watermark(self, value):
        """Save new watermark."""
        if value is None:
            return

        try:
            path = self._get_state_path()
            state = {}
            if path.exists():
                try:
                    with open(path) as f:
                        state = json.load(f)
                except Exception:
                    pass  # Corrupt or empty

            if self.pipeline_name not in state:
                state[self.pipeline_name] = {}

            state[self.pipeline_name]["watermark"] = value
            state[self.pipeline_name]["last_run"] = self.clock()

            with open(path, "w") as f:
                json.dump(state, f, indent=2)

            logger.info(f"Saved new watermark: {value}")
        except Exception as e:
            logger.warning(f"Failed to save state: {e}")

    def run(self) -> dict[str, Any]:
        """
        Execute the pipeline on an engine this runner opens, with comprehensive error handling.

        Raises:
            PipelineExecutionError: If any stage fails
        """
        logger.info("=" * 60)
        logger.info(f"Starting pipeline execution: {self.pipeline_name}")
        logger.info(f"Source: {self.config.source.type}")
        logger.info(f"Target: {self.config.target.type}")
        logger.info("=" * 60)

        self._report_progress(5, "🔧 Initializing adapters...")
        start_time = time.perf_counter()

        try:
            with DuckDBEngine(
                threads=self.options.threads, memory_limit=self.options.memory_limit
            ) as con:
                return self._run_stages(con, start_time)

        except AdapterError as e:
            logger.exception(f"Adapter error: {e}")
            raise PipelineExecutionError(f"Adapter error: {e}") from e
        except Exception as e:
            logger.exception(f"Pipeline execution failed: {e}")
            raise PipelineExecutionError(f"Pipeline failed: {e}") from e

    def run_on(self, con: DuckDBPyConnection) -> dict[str, Any]:
        """
        Execute the pipeline on a connection the caller opened.

        This is the seam a test drives the runner through, with DuckDB in process. The caller
        loads whatever extension the source and the target need.

        Args:
            con: DuckDB connection

        Returns:
            The same results as `run`

        Raises:
            PipelineExecutionError: If a count, sample, summary or write stage fails
            Exception: Any other error, as the stage raised it. `run` wraps these, `run_on`
                does not.
        """
        return self._run_stages(con, time.perf_counter())

    def _run_stages(self, con, start_time: float) -> dict[str, Any]:
        """Attach, check the schema, profile, write, and save the watermark."""
        source_adapter, target_adapter = self._attach(con)

        # Get source relation SQL
        base_relation = source_adapter.get_relation_sql()
        logger.debug(f"Base relation: {base_relation}")

        # 1. Sync Schema (Evolve). It returns the columns the write names.
        logger.info("Checking schema synchronization...")
        columns = target_adapter.sync_schema(
            con, base_relation, evolution_override=self.options.schema_evolution
        )

        # 2. Get Watermark & Apply Incremental Filter
        relation_sql = self._incremental_relation(source_adapter, base_relation)

        # Stages 1 to 3: count, sample, summarize
        results = self._profile(con, relation_sql)

        # Stage 4: Write to target
        results.update(self._load(con, target_adapter, relation_sql, columns))

        total_time = time.perf_counter() - start_time
        results["timings"] = self._timings(total_time)

        written = results["written"]
        logger.info("=" * 60)
        logger.info(f"Pipeline completed successfully in {total_time:.2f}s")
        logger.info(f"Rows processed: {results['rows']}")
        logger.info(f"Rows written: {written['rows']}, bytes written: {written['bytes']}")
        logger.info("=" * 60)

        return results

    def _load(
        self, con, target_adapter: TargetAdapter, relation_sql: str, columns: list[str]
    ) -> dict[str, Any]:
        """Write the batch and save its watermark. Returns the statement and what it moved."""
        write_sql = target_adapter.build_write_sql(relation_sql, columns)
        new_watermark = self._next_watermark(con, relation_sql)

        self._report_progress(50, "🚀 Writing data...")
        written = self._execute_write(con, target_adapter, write_sql)

        # Save watermark after successful write
        if new_watermark is not None:
            self._save_watermark(new_watermark)

        return {
            "write_sql": write_sql.strip(),
            "written": {"rows": written.rows, "bytes": written.bytes},
        }

    def _attach(self, con) -> tuple[SourceAdapter, TargetAdapter]:
        """Create the two adapters and attach what each one needs."""
        logger.info("Initializing adapters...")
        source_adapter = create_source_adapter(self.config.source.model_dump())
        target_adapter = create_target_adapter(self.config.target.model_dump())

        self._report_progress(10, "🔗 Attaching sources...")

        logger.info("Attaching data sources...")
        source_adapter.attach(con)
        target_adapter.attach(con)
        return source_adapter, target_adapter

    def _incremental_relation(self, source_adapter: SourceAdapter, base_relation: str) -> str:
        """The source relation, filtered by the saved watermark when one applies."""
        watermark = (
            None
            if self.options.full_refresh or self.options.ignore_watermark
            else self._get_watermark()
        )
        relation_sql = source_adapter.get_incremental_sql(base_relation, watermark)

        if relation_sql != base_relation:
            logger.info(f"Incremental filtering applied (watermark: {watermark})")
        else:
            logger.info("No incremental filtering applied.")
        return relation_sql

    def _profile(self, con, relation_sql: str) -> dict[str, Any]:
        """Run the stages the options turn on: count, sample, summarize."""
        results: dict[str, Any] = {}

        # Stage 1: Count rows
        # Note: This counts rows *after* filtering
        if self.options.compute_counts:
            self._report_progress(20, "🔢 Counting rows...")
            results["rows"] = self._count_rows(con, relation_sql)
        else:
            results["rows"] = "N/A"

        # Stage 2: Sample data
        if self.options.sample_data:
            self._report_progress(30, "🔍 Sampling data...")
            results["sample"] = self._sample_data(con, relation_sql)
        else:
            results["sample"] = None

        # Stage 3: Summarize data
        if self.options.compute_summary:
            self._report_progress(40, "📊 Generating summary...")
            results["summary"] = self._summarize_data(con, relation_sql)
        else:
            results["summary"] = None
        return results

    def _next_watermark(self, con, relation_sql: str) -> Any:
        """The highest incremental key in the batch about to be written, read by its own scan."""
        key = self.config.source.incremental_key
        if not key:
            return None
        try:
            # MAX handles an empty set (returns NULL). Use the filtered relation_sql.
            return con.execute(f"SELECT MAX({key}) FROM {relation_sql}").fetchone()[0]
        except Exception as e:
            logger.warning(f"Failed to calculate new watermark: {e}")
            return None

    def _timings(self, total_time: float) -> dict[str, float]:
        """Seconds spent in each stage, and in the whole run."""
        return {
            "count_s": self.metrics.get("count_s", 0.0),
            "sample_s": self.metrics.get("sample_s", 0.0),
            "summary_s": self.metrics.get("summary_s", 0.0),
            "write_s": self.metrics.get("write_s", 0.0),
            "total_s": round(total_time, 4),
        }

    def _count_rows(self, con, relation_sql: str) -> int:
        """Count rows in source with error handling."""
        try:
            logger.info("Counting rows...")
            start = time.perf_counter()

            count = con.execute(f"SELECT COUNT(*) FROM {relation_sql}").fetchone()[0]

            elapsed = time.perf_counter() - start
            self.metrics["count_s"] = round(elapsed, 4)

            logger.info(f"Row count: {count:,} ({elapsed:.2f}s)")
            return count
        except Exception as e:
            logger.error(f"Failed to count rows: {e}")
            raise PipelineExecutionError(f"Row count failed: {e}") from e

    def _sample_data(self, con, relation_sql: str):
        """Sample data from source with error handling."""
        try:
            logger.info(f"Sampling {self.options.sample_rows} rows...")
            start = time.perf_counter()

            sample = con.execute(
                f"SELECT * FROM {relation_sql} LIMIT {self.options.sample_rows};"
            ).fetchdf()

            elapsed = time.perf_counter() - start
            self.metrics["sample_s"] = round(elapsed, 4)

            logger.info(f"Sampled {len(sample)} rows ({elapsed:.2f}s)")
            return sample
        except Exception as e:
            logger.error(f"Failed to sample data: {e}")
            raise PipelineExecutionError(f"Data sampling failed: {e}") from e

    def _summarize_data(self, con, relation_sql: str):
        """Generate summary statistics with error handling."""
        try:
            logger.info("Generating summary statistics...")
            start = time.perf_counter()

            summary = con.execute(f"SUMMARIZE SELECT * FROM {relation_sql};").fetchdf()

            elapsed = time.perf_counter() - start
            self.metrics["summary_s"] = round(elapsed, 4)

            logger.info(f"Generated summary ({elapsed:.2f}s)")
            return summary
        except Exception as e:
            logger.error(f"Failed to generate summary: {e}")
            raise PipelineExecutionError(f"Summary generation failed: {e}") from e

    def _execute_write(self, con, target_adapter: TargetAdapter, write_sql: str) -> WriteReport:
        """Execute the write and return what it moved, counted by the write itself."""
        try:
            logger.info("Writing data to target...")
            start = time.perf_counter()

            # The adapter runs the statement in a transaction, for atomicity
            written = target_adapter.write(con, write_sql)

            elapsed = time.perf_counter() - start
            self.metrics["write_s"] = round(elapsed, 4)

            logger.info(f"Write completed ({elapsed:.2f}s)")
        except Exception as e:
            logger.error(f"Failed to write data: {e}")
            raise PipelineExecutionError(f"Write operation failed: {e}") from e

        if Counted.BYTES not in target_adapter.counts:
            logger.warning(
                f"A {self.config.target.type} target does not count bytes: "
                f"the write returned {written.rows} rows and no byte count."
            )
        return written


def run_pipeline(p: dict, overrides: dict = None) -> dict:
    """Legacy function for backward compatibility."""
    config = PipelineConfig(**p)
    runner = PipelineRunner(config, overrides)
    return runner.run()
