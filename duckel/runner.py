"""
Pipeline execution runner with comprehensive error handling.
"""
import time
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
    
    Usage:
        config = PipelineConfig(**pipeline_dict)
        runner = PipelineRunner(config, overrides={"sample_rows": 100})
        result = runner.run()
    """
    
    def __init__(
        self,
        pipeline_config: PipelineConfig,
        overrides: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize pipeline runner.
        
        Args:
            pipeline_config: Validated PipelineConfig instance
            overrides: Optional dictionary of runtime option overrides
        """
        self.config = pipeline_config
        self.options = pipeline_config.get_options(overrides)
        self.metrics: Dict[str, float] = {}
        
    def run(self) -> Dict[str, Any]:
        """
        Execute the pipeline with comprehensive error handling.
        
        Returns:
            Dictionary containing execution results and metrics
            
        Raises:
            PipelineExecutionError: If pipeline execution fails
        """
        logger.info("=" * 60)
        logger.info("Starting pipeline execution")
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
                relation_sql = source_adapter.get_relation_sql()
                logger.debug(f"Source relation: {relation_sql}")
                
                # Execute pipeline stages
                results = {}
                
                # Stage 1: Count rows
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
                self._execute_write(con, write_sql)
                
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
        """
        Count rows in source with error handling.
        
        Args:
            con: DuckDB connection
            relation_sql: SQL expression for source relation
            
        Returns:
            Row count
        """
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
        """
        Sample data from source with error handling.
        
        Args:
            con: DuckDB connection
            relation_sql: SQL expression for source relation
            
        Returns:
            Pandas DataFrame with sample data
        """
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
        """
        Generate summary statistics with error handling.
        
        Args:
            con: DuckDB connection
            relation_sql: SQL expression for source relation
            
        Returns:
            Pandas DataFrame with summary statistics
        """
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
        """
        Execute write to target with transaction support.
        
        Args:
            con: DuckDB connection
            write_sql: SQL to write data to target
        """
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


# ===== LEGACY COMPATIBILITY FUNCTION =====

def run_pipeline(p: dict, overrides: dict = None) -> dict:
    """
    Legacy function for backward compatibility.
    
    This function maintains compatibility with old code that doesn't use
    PipelineConfig validation.
    
    Args:
        p: Pipeline dictionary
        overrides: Optional runtime overrides
        
    Returns:
        Execution results dictionary
    """
    # Convert dict to PipelineConfig for validation
    config = PipelineConfig(**p)
    
    # Run with new PipelineRunner
    runner = PipelineRunner(config, overrides)
    return runner.run()
