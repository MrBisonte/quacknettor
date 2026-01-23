"""
Scheduler manager for coordinating pipeline execution jobs.
Using APScheduler's BackgroundScheduler.
"""
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.cron import CronTrigger
from datetime import datetime
import logging

logger = logging.getLogger("duckel.scheduler")

class SchedulerManager:
    """
    Singleton-like wrapper for BackgroundScheduler.
    """
    def __init__(self):
        self.scheduler = BackgroundScheduler()
        self.scheduler.start()
        logger.info("Scheduler started")

    def schedule_pipeline_run(self, run_func, run_args, run_at: datetime = None, cron_expr: str = None, job_id: str = None):
        """
        Schedule a pipeline run.
        
        Args:
            run_func: The callable to execute (should be pickleable/global)
            run_args: Arguments to pass to run_func
            run_at: Datetime for one-off execution
            cron_expr: Cron string (e.g. "*/5 * * * *")
            job_id: Unique ID for the job
        """
        trigger = None
        if run_at:
            trigger = DateTrigger(run_date=run_at)
        elif cron_expr:
            # Simple cron parsing: assume standard unix 5-part cron, or let APScheduler parse kwargs if broken down
            # For simplicity, we'll try to parse a string "min hour day month dow"
            # But better to just let user pass explicit args? 
            # Or use CronTrigger.from_crontab(cron_expr)
            trigger = CronTrigger.from_crontab(cron_expr)
            
        if not trigger:
            raise ValueError("Must provide either run_at or cron_expr")

        job = self.scheduler.add_job(
            run_func,
            trigger=trigger,
            args=[run_args], # Passing the whole config dict as one arg? Or expanded?
            # run_func likely expects (pipeline_config_dict). 
            # We will standardize on passing a single DICT configuration.
            id=job_id,
            replace_existing=True
        )
        logger.info(f"Scheduled job {job_id} at {trigger}")
        return job

    def get_jobs(self):
        """Return list of active jobs."""
        return self.scheduler.get_jobs()

    def remove_job(self, job_id):
        self.scheduler.remove_job(job_id)

    def shutdown(self):
        self.scheduler.shutdown()
