import sqlite3
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Callable
from contextlib import contextmanager
from enum import Enum
import threading
import time
import yaml
from pathlib import Path

class JobType(Enum):
    HEALTH_CHECK = "health_check"
    ASSET_DISCOVERY = "asset_discovery"
    DATA_QUALITY = "data_quality"
    METADATA_SYNC = "metadata_sync"
    ALERT_PROCESSING = "alert_processing"

class JobStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"

class AutomatedJobScheduler:
    """Automated job scheduler for maintenance tasks"""
    
    def __init__(self, db_path: str = "scheduler.db", config_path: str = "config/app_config.yaml"):
        self.db_path = db_path
        self.config_path = config_path
        self.config = self._load_config()
        self.init_db()
        self.jobs_registry = {}
        self.running = False
        self.scheduler_thread = None
    
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from YAML file"""
        try:
            config_file = Path(self.config_path)
            if config_file.exists():
                with open(config_file, 'r') as f:
                    return yaml.safe_load(f) or {}
        except:
            pass
        return {}
    
    @contextmanager
    def get_connection(self):
        """Context manager for database connections"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()
    
    def init_db(self):
        """Initialize the job scheduler database"""
        with self.get_connection() as conn:
            # Scheduled jobs table
            conn.execute('''
                CREATE TABLE IF NOT EXISTS scheduled_jobs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_name TEXT UNIQUE,
                    job_type TEXT,
                    schedule_expression TEXT,  -- cron-like expression
                    enabled BOOLEAN DEFAULT TRUE,
                    last_run TIMESTAMP,
                    next_run TIMESTAMP,
                    config TEXT,  -- JSON serialized dict
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Job execution history table
            conn.execute('''
                CREATE TABLE IF NOT EXISTS job_executions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_id INTEGER,
                    status TEXT,
                    start_time TIMESTAMP,
                    end_time TIMESTAMP,
                    result TEXT,  -- JSON serialized dict
                    error_message TEXT,
                    FOREIGN KEY (job_id) REFERENCES scheduled_jobs (id)
                )
            ''')
            
            conn.commit()
    
    def register_job(self, job_name: str, job_type: JobType, schedule_expression: str, 
                    job_function: Callable, config: Optional[Dict[str, Any]] = None):
        """Register a job with the scheduler"""
        self.jobs_registry[job_name] = {
            "function": job_function,
            "type": job_type,
            "schedule": schedule_expression,
            "config": config or {}
        }
        
        # Save to database
        with self.get_connection() as conn:
            # Check if job already exists
            existing = conn.execute(
                'SELECT id FROM scheduled_jobs WHERE job_name = ?',
                (job_name,)
            ).fetchone()
            
            if existing:
                # Update existing job
                conn.execute('''
                    UPDATE scheduled_jobs 
                    SET job_type = ?, schedule_expression = ?, config = ?
                    WHERE job_name = ?
                ''', (
                    job_type.value,
                    schedule_expression,
                    json.dumps(config) if config else None,
                    job_name
                ))
            else:
                # Insert new job
                next_run = self._calculate_next_run(schedule_expression)
                conn.execute('''
                    INSERT INTO scheduled_jobs 
                    (job_name, job_type, schedule_expression, next_run, config)
                    VALUES (?, ?, ?, ?, ?)
                ''', (
                    job_name,
                    job_type.value,
                    schedule_expression,
                    next_run.isoformat() if next_run else None,
                    json.dumps(config) if config else None
                ))
            
            conn.commit()
    
    def _calculate_next_run(self, schedule_expression: str) -> Optional[datetime]:
        """Calculate next run time based on schedule expression"""
        # This is a simplified implementation
        # In a real system, you'd parse cron expressions or use a library like croniter
        try:
            # For now, we'll support simple minute-based intervals
            # Format: "@every X minutes" or "@every X hours"
            if schedule_expression.startswith("@every "):
                parts = schedule_expression.split()
                if len(parts) >= 3:
                    value = int(parts[1])
                    unit = parts[2].lower()
                    
                    now = datetime.utcnow()
                    if unit.startswith("minute"):
                        return now + timedelta(minutes=value)
                    elif unit.startswith("hour"):
                        return now + timedelta(hours=value)
                    elif unit.startswith("day"):
                        return now + timedelta(days=value)
        except:
            pass
        
        # Default to 15 minutes from now
        return datetime.utcnow() + timedelta(minutes=15)
    
    def start_scheduler(self):
        """Start the job scheduler"""
        if self.running:
            return
            
        # Check if scheduler is enabled in config
        if not self.config.get('job_scheduler', {}).get('enabled', True):
            print("Job scheduler is disabled in configuration")
            return
            
        self.running = True
        self.scheduler_thread = threading.Thread(target=self._scheduler_loop, daemon=True)
        self.scheduler_thread.start()
        print("Job scheduler started")
    
    def stop_scheduler(self):
        """Stop the job scheduler"""
        self.running = False
        if self.scheduler_thread:
            self.scheduler_thread.join()
        print("Job scheduler stopped")
    
    def _scheduler_loop(self):
        """Main scheduler loop"""
        while self.running:
            try:
                self._execute_due_jobs()
                time.sleep(60)  # Check every minute
            except Exception as e:
                print(f"Scheduler error: {e}")
                time.sleep(60)
    
    def _execute_due_jobs(self):
        """Execute jobs that are due to run"""
        now = datetime.utcnow()
        
        with self.get_connection() as conn:
            # Get jobs that are due to run
            rows = conn.execute('''
                SELECT * FROM scheduled_jobs 
                WHERE enabled = TRUE AND next_run <= ?
            ''', (now.isoformat(),)).fetchall()
            
            for row in rows:
                job_id = row['id']
                job_name = row['job_name']
                
                # Update job status to running
                execution_id = self._start_job_execution(job_id)
                
                try:
                    # Execute the job
                    if job_name in self.jobs_registry:
                        job_info = self.jobs_registry[job_name]
                        result = job_info["function"](**job_info["config"])
                        
                        # Mark job as completed
                        self._complete_job_execution(execution_id, result)
                    else:
                        # Mark job as failed
                        self._fail_job_execution(execution_id, "Job function not found")
                    
                    # Calculate next run time
                    next_run = self._calculate_next_run(row['schedule_expression'])
                    if next_run:
                        conn.execute('''
                            UPDATE scheduled_jobs 
                            SET last_run = ?, next_run = ?
                            WHERE id = ?
                        ''', (now.isoformat(), next_run.isoformat(), job_id))
                        conn.commit()
                        
                except Exception as e:
                    # Mark job as failed
                    self._fail_job_execution(execution_id, str(e))
                    
                    # Still update next run time even if job failed
                    next_run = self._calculate_next_run(row['schedule_expression'])
                    if next_run:
                        conn.execute('''
                            UPDATE scheduled_jobs 
                            SET last_run = ?, next_run = ?
                            WHERE id = ?
                        ''', (now.isoformat(), next_run.isoformat(), job_id))
                        conn.commit()
    
    def _start_job_execution(self, job_id: int) -> int:
        """Start a job execution and return execution ID"""
        with self.get_connection() as conn:
            conn.execute('''
                INSERT INTO job_executions 
                (job_id, status, start_time)
                VALUES (?, ?, ?)
            ''', (job_id, JobStatus.RUNNING.value, datetime.utcnow().isoformat()))
            conn.commit()
            
            # Get the execution ID
            execution_id = conn.execute(
                'SELECT id FROM job_executions WHERE job_id = ? ORDER BY id DESC LIMIT 1',
                (job_id,)
            ).fetchone()['id']
            
            return execution_id
    
    def _complete_job_execution(self, execution_id: int, result: Any):
        """Mark a job execution as completed"""
        with self.get_connection() as conn:
            conn.execute('''
                UPDATE job_executions 
                SET status = ?, end_time = ?, result = ?
                WHERE id = ?
            ''', (
                JobStatus.COMPLETED.value,
                datetime.utcnow().isoformat(),
                json.dumps(result) if result else None,
                execution_id
            ))
            conn.commit()
    
    def _fail_job_execution(self, execution_id: int, error_message: str):
        """Mark a job execution as failed"""
        with self.get_connection() as conn:
            conn.execute('''
                UPDATE job_executions 
                SET status = ?, end_time = ?, error_message = ?
                WHERE id = ?
            ''', (
                JobStatus.FAILED.value,
                datetime.utcnow().isoformat(),
                error_message,
                execution_id
            ))
            conn.commit()
    
    def get_job_status(self, job_name: str) -> Optional[Dict[str, Any]]:
        """Get the status of a specific job"""
        with self.get_connection() as conn:
            row = conn.execute('''
                SELECT sj.*, je.status as last_execution_status, je.start_time as last_execution_start
                FROM scheduled_jobs sj
                LEFT JOIN job_executions je ON sj.id = je.job_id AND je.start_time = (
                    SELECT MAX(start_time) FROM job_executions WHERE job_id = sj.id
                )
                WHERE sj.job_name = ?
            ''', (job_name,)).fetchone()
            
            if row:
                return {
                    "job_name": row["job_name"],
                    "job_type": row["job_type"],
                    "schedule_expression": row["schedule_expression"],
                    "enabled": bool(row["enabled"]),
                    "last_run": row["last_run"],
                    "next_run": row["next_run"],
                    "last_execution_status": row["last_execution_status"],
                    "last_execution_start": row["last_execution_start"],
                    "created_at": row["created_at"]
                }
            return None
    
    def get_all_jobs(self) -> List[Dict[str, Any]]:
        """Get status of all jobs"""
        with self.get_connection() as conn:
            rows = conn.execute('''
                SELECT sj.*, je.status as last_execution_status, je.start_time as last_execution_start
                FROM scheduled_jobs sj
                LEFT JOIN job_executions je ON sj.id = je.job_id AND je.start_time = (
                    SELECT MAX(start_time) FROM job_executions WHERE job_id = sj.id
                )
            ''').fetchall()
            
            return [{
                "job_name": row["job_name"],
                "job_type": row["job_type"],
                "schedule_expression": row["schedule_expression"],
                "enabled": bool(row["enabled"]),
                "last_run": row["last_run"],
                "next_run": row["next_run"],
                "last_execution_status": row["last_execution_status"],
                "last_execution_start": row["last_execution_start"],
                "created_at": row["created_at"]
            } for row in rows]
    
    def initialize_default_jobs(self):
        """Initialize default jobs from configuration"""
        try:
            # Import job functions here to avoid circular imports
            from my_dagster_project.jobs.automated_jobs import run_health_checks, run_asset_discovery, process_alerts
            
            # Get job scheduler config
            scheduler_config = self.config.get('job_scheduler', {})
            
            # Register health check job
            self.register_job(
                job_name="health_check_job",
                job_type=JobType.HEALTH_CHECK,
                schedule_expression=scheduler_config.get('health_check_job', '@every 15 minutes'),
                job_function=run_health_checks
            )
            
            # Register asset discovery job
            self.register_job(
                job_name="asset_discovery_job",
                job_type=JobType.ASSET_DISCOVERY,
                schedule_expression=scheduler_config.get('discovery_job', '@every 1 hour'),
                job_function=run_asset_discovery
            )
            
            # Register alert processing job
            self.register_job(
                job_name="alert_processing_job",
                job_type=JobType.ALERT_PROCESSING,
                schedule_expression=scheduler_config.get('alert_processing_job', '@every 5 minutes'),
                job_function=process_alerts
            )
            
            print("Default jobs initialized")
            
        except Exception as e:
            print(f"Failed to initialize default jobs: {e}")

# Global instance
job_scheduler = AutomatedJobScheduler()