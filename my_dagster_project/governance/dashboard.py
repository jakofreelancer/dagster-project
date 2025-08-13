import pandas as pd
from typing import Dict, List, Any
from datetime import datetime, timedelta

from my_dagster_project.core.db_config import db_config
from my_dagster_project.core.logging_config import get_logger

logger = get_logger(__name__)

class GovernanceDashboard:
  """Asset governance and monitoring dashboard"""
  
  def __init__(self):
    self.db = db_config
  
  def get_asset_inventory(self) -> pd.DataFrame:
    """Get complete asset inventory"""
    
    with self.db.get_connection('metadata_db') as conn:
      query = """
        SELECT 
          asset_key,
          asset_name,
          asset_type,
          group_name,
          pipeline_name,
          owners,
          tags,
          auto_discovered,
          is_active,
          created_date,
          modified_date
        FROM asset_registry
        ORDER BY pipeline_name, asset_key
      """
      
      return pd.read_sql(query, conn)
  
  def get_pipeline_health(self) -> pd.DataFrame:
    """Get health metrics for all pipelines"""
    
    from my_dagster_project.core.monitoring import monitor
    health_data = monitor.get_pipeline_health()
    
    return pd.DataFrame(health_data)
  
  def get_execution_history(self, asset_key: str = None, days: int = 7) -> pd.DataFrame:
    """Get execution history for assets"""
    
    with self.db.get_connection('metadata_db') as conn:
      query = """
        SELECT 
          asset_key,
          run_id,
          window_start,
          window_end,
          start_time,
          end_time,
          status,
          records_processed,
          records_failed,
          DATEDIFF(SECOND, start_time, end_time) as duration_seconds
        FROM asset_execution
        WHERE start_time >= DATEADD(DAY, -?, GETDATE())
      """
      
      params = [days]
      
      if asset_key:
        query += " AND asset_key = ?"
        params.append(asset_key)
      
      query += " ORDER BY start_time DESC"
      
      return pd.read_sql(query, conn, params=params)
  
  def generate_health_report(self) -> Dict[str, Any]:
    """Generate comprehensive health report"""
    
    health_df = self.get_pipeline_health()
    
    return {
      'total_assets': len(health_df),
      'healthy_assets': len(health_df[health_df['health_status'] == 'HEALTHY']),
      'unhealthy_assets': len(health_df[health_df['health_status'] == 'UNHEALTHY']),
      'stale_assets': len(health_df[health_df['health_status'] == 'STALE']),
      'never_run_assets': len(health_df[health_df['health_status'] == 'NEVER_RUN']),
      'report_time': datetime.now().isoformat(),
      'details': health_df.to_dict('records')
    }
