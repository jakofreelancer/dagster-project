from typing import Dict, Any
from datetime import datetime
import pyodbc

from my_dagster_project.core.db_config import db_config
from my_dagster_project.core.logging_config import get_logger

logger = get_logger(__name__)

class PipelineMonitor:
  """Pipeline monitoring and metrics"""
  
  def __init__(self):
    self.db = db_config
  
  def record_metric(
    self,
    pipeline_name: str,
    metric_name: str,
    metric_value: float,
    asset_key: str = None
  ):
    """Record a pipeline metric"""
    
    with self.db.get_connection('metadata_db') as conn:
      cursor = conn.cursor()
      
      cursor.execute("""
        INSERT INTO pipeline_metrics 
        (pipeline_name, asset_key, metric_name, metric_value)
        VALUES (?, ?, ?, ?)
      """,
        pipeline_name,
        asset_key,
        metric_name,
        metric_value
      )
      
      conn.commit()
  
  def get_pipeline_health(self) -> Dict[str, Any]:
    """Get pipeline health metrics"""
    
    with self.db.get_connection('metadata_db') as conn:
      cursor = conn.cursor()
      
      cursor.execute("""
        WITH recent_executions AS (
          SELECT 
            asset_key,
            COUNT(*) as total_runs,
            SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as successful_runs,
            AVG(duration_seconds) as avg_duration,
            MAX(end_time) as last_run
          FROM asset_execution
          WHERE start_time >= DATEADD(DAY, -7, GETDATE())
          GROUP BY asset_key
        )
        SELECT 
          ar.asset_key,
          ar.asset_name,
          ar.pipeline_name,
          ISNULL(re.total_runs, 0) as runs_last_7d,
          ISNULL(re.successful_runs, 0) as successful_runs,
          ISNULL(re.avg_duration, 0) as avg_duration_seconds,
          re.last_run,
          CASE 
            WHEN re.last_run IS NULL THEN 'NEVER_RUN'
            WHEN re.last_run < DATEADD(DAY, -1, GETDATE()) THEN 'STALE'
            WHEN re.successful_runs < re.total_runs * 0.9 THEN 'UNHEALTHY'
            ELSE 'HEALTHY'
          END as health_status
        FROM asset_registry ar
        LEFT JOIN recent_executions re ON ar.asset_key = re.asset_key
        WHERE ar.is_active = 1
        ORDER BY health_status DESC, ar.asset_key
      """)
      
      results = []
      for row in cursor.fetchall():
        results.append({
          'asset_key': row[0],
          'asset_name': row[1],
          'pipeline_name': row[2],
          'runs_last_7d': row[3],
          'successful_runs': row[4],
          'avg_duration': row[5],
          'last_run': row[6],
          'health_status': row[7]
        })
      
      return results

# Global instance
monitor = PipelineMonitor()
