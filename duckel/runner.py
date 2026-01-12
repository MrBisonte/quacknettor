"""
Pipeline execution runner with comprehensive error handling.
"""
import time
import json
import os
from pathlib import Path
from typing import Dict, Any, Optional
from .engine import DuckDBEngine
from .adapters import create_source_adapter, create_target_adapter, AdapterError
from .models import PipelineConfig
from .logger import logger


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
        overrides: Optional[Dict[str, Any]] = None,
        pipeline_name: str = "default"
    ):
        """
        Initialize pipeline runner.
        
        Args:
            pipeline_config: Validated PipelineConfig instance
            overrides: Optional dictionary of runtime option overrides
            pipeline_name: Name of the pipeline for state persistence
        """
        self.config = pipeline_config
        self.options = pipeline_config.get_options(overrides)
        self.pipeline_name = pipeline_name
        self.metrics: Dict[str, float] = {}

    def _get_state_path(self) -> Path:
        """Get path to state file."""
        # Simple local state file. In production this might be S3/DB.
        return Path(".duckel_state.json")

    def _get_watermark(self) -> Any:
        """Get last processed watermark for this pipeline."""
        try:
            path = self._get_state_path()
            if path.exists():
                with open(path, "r") as f:
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
                    with open(path, "r") as f:
                        state = json.load(f)
                except Exception:
                    pass # Corrupt or empty
            
            if self.pipeline_name not in state:
                state[self.pipeline_name] = {}
            
            state[self.pipeline_name]["watermark"] = value
            state[self.pipeline_name]["last_run"] = time.time()
            
            with open(path, "w") as f:
                json.dump(state, f, indent=2)
                
            logger.info(f"Saved new watermark: {value}")
        except Exception as e:
            logger.warning(f"Failed to save state: {e}")
        
    def run(self) -> Dict[str, Any]:
        """
        Execute the pipeline with comprehensive error handling.
        """
        logger.info("=" * 60)
        logger.info(f"Starting pipeline execution: {self.pipeline_name}")
        logger.info(f"Source: {self.config.source.type}")
        logger.info(f"Target: {self.config.target.type}")
        logger.info("=" * 60)
        
        start_time = time.perf_counter()
        
        try:
            with DuckDBEngine(
                threads=self.options.threads,
                memory_limit=self.options.memory_limit
            ) as con:
                # Initialize adapters
                logger.info("Initializing adapters...")
                source_adapter = create_source_adapter(self.config.source.model_dump())
                target_adapter = create_target_adapter(self.config.target.model_dump())
                
                # Attach sources and targets
                logger.info("Attaching data sources...")
                source_adapter.attach(con)
                target_adapter.attach(con)
                
                # Get source relation SQL
                base_relation = source_adapter.get_relation_sql()
                logger.debug(f"Base relation: {base_relation}")
                
                # 1. Sync Schema (Evolve)
                logger.info("Checking schema synchronization...")
                target_adapter.sync_schema(con, base_relation, evolution_override=self.options.schema_evolution)
                
                # 2. Get Watermark & Apply Incremental Filter
                watermark = None if self.options.full_refresh or self.options.ignore_watermark else self._get_watermark()
                relation_sql = source_adapter.get_incremental_sql(base_relation, watermark)
                
                if relation_sql != base_relation:
                    logger.info(f"Incremental filtering applied (watermark: {watermark})")
                else:
                    logger.info("No incremental filtering applied.")
                
                # Execute pipeline stages
                results = {}
                
                # Stage 1: Count rows
                # Note: This counts rows *after* filtering
                if self.options.compute_counts:
                    results["rows"] = self._count_rows(con, relation_sql)
                else:
                    results["rows"] = "N/A"
                
                # Stage 2: Sample data
                if self.options.sample_data:
                    results["sample"] = self._sample_data(con, relation_sql)
                else:
                    results["sample"] = None
                
                # Stage 3: Summarize data
                if self.options.compute_summary:
                    results["summary"] = self._summarize_data(con, relation_sql)
                else:
                    results["summary"] = None
                
                # Stage 4: Write to target
                write_sql = target_adapter.build_write_sql(relation_sql)
                results["write_sql"] = write_sql.strip()
                
                # Determine new watermark *before* write or *during* transaction?
                # Calculating max value from the batch we are about to write.
                new_watermark = None
                if self.config.source.incremental_key:
                    try:
                        key = self.config.source.incremental_key
                        # Ensure we don't scan if 0 rows, but MAX handles empty set (returns NULL)
                        # Use the filtered relation_sql
                        wm_query = f"SELECT MAX({key}) FROM {relation_sql}"
                        new_watermark = con.execute(wm_query).fetchone()[0]
                    except Exception as e:
                        logger.warning(f"Failed to calculate new watermark: {e}")

                self._execute_write(con, write_sql)
                
                # Save watermark after successful write
                if new_watermark is not None:
                    self._save_watermark(new_watermark)
                
                # Calculate timings
                total_time = time.perf_counter() - start_time
                results["timings"] = {
                    "count_s": self.metrics.get("count_s", 0.0),
                    "sample_s": self.metrics.get("sample_s", 0.0),
                    "summary_s": self.metrics.get("summary_s", 0.0),
                    "write_s": self.metrics.get("write_s", 0.0),
                    "total_s": round(total_time, 4),
                }
                
                logger.info("=" * 60)
                logger.info(f"Pipeline completed successfully in {total_time:.2f}s")
                logger.info(f"Rows processed: {results['rows']}")
                logger.info("=" * 60)
                
                return results
                
        except AdapterError as e:
            logger.exception(f"Adapter error: {e}")
            raise PipelineExecutionError(f"Adapter error: {e}") from e
        except Exception as e:
            logger.exception(f"Pipeline execution failed: {e}")
            raise PipelineExecutionError(f"Pipeline failed: {e}") from e
    
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
    
    def _execute_write(self, con, write_sql: str):
        """Execute write to target with transaction support."""
        try:
            logger.info("Writing data to target...")
            start = time.perf_counter()
            
            # Begin transaction for atomicity
            con.execute("BEGIN TRANSACTION;")
            
            try:
                con.execute(write_sql)
                con.execute("COMMIT;")
                
                elapsed = time.perf_counter() - start
                self.metrics["write_s"] = round(elapsed, 4)
                
                logger.info(f"Write completed ({elapsed:.2f}s)")
            except Exception as e:
                logger.error(f"Write failed, rolling back transaction: {e}")
                con.execute("ROLLBACK;")
                raise
                
        except Exception as e:
            logger.error(f"Failed to write data: {e}")
            raise PipelineExecutionError(f"Write operation failed: {e}") from e


def run_pipeline(p: dict, overrides: dict = None) -> dict:
    """Legacy function for backward compatibility."""
    config = PipelineConfig(**p)
    runner = PipelineRunner(config, overrides)
    return runner.run()
