import pandas as pd
from typing import Dict, List, Any
from datetime import datetime, timedelta

try:
  from my_dagster_project.core.db_config import db_config
  from my_dagster_project.core.logging_config import get_logger
  logger = get_logger(__name__)
  ENTERPRISE_ENABLED = True
except ImportError:
  ENTERPRISE_ENABLED = False
  import logging
  logger = logging.getLogger(__name__)

class GovernanceDashboard:
  """Asset governance and monitoring dashboard"""
  
  def __init__(self):
    if ENTERPRISE_ENABLED:
      self.db = db_config
    else:
      self.db = None
  
  def get_asset_inventory(self) -> pd.DataFrame:
    """Get complete asset inventory"""
    if not ENTERPRISE_ENABLED or not self.db:
        return pd.DataFrame()
    
    try:
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
    except Exception as e:
      logger.error(f"Error getting asset inventory: {e}")
      return pd.DataFrame()
  
  def get_pipeline_health(self) -> pd.DataFrame:
    """Get health metrics for all pipelines"""
    if not ENTERPRISE_ENABLED:
      return pd.DataFrame()
    
    try:
      from my_dagster_project.core.monitoring import monitor
      health_data = monitor.get_pipeline_health()
      return pd.DataFrame(health_data)
    except Exception as e:
      logger.error(f"Error getting pipeline health: {e}")
      return pd.DataFrame()
  
  def generate_health_report(self) -> Dict[str, Any]:
    """Generate comprehensive health report"""
    if not ENTERPRISE_ENABLED:
      return {
        'total_assets': 0,
        'healthy_assets': 0,
        'unhealthy_assets': 0,
        'stale_assets': 0,
        'never_run_assets': 0,
        'report_time': datetime.now().isoformat(),
        'details': []
      }
    
    try:
      health_df = self.get_pipeline_health()
      
      if health_df.empty:
        return {
          'total_assets': 0,
          'healthy_assets': 0,
          'unhealthy_assets': 0,
          'stale_assets': 0,
          'never_run_assets': 0,
          'report_time': datetime.now().isoformat(),
          'details': []
        }
      
      return {
        'total_assets': len(health_df),
        'healthy_assets': len(health_df[health_df['health_status'] == 'HEALTHY']),
        'unhealthy_assets': len(health_df[health_df['health_status'] == 'UNHEALTHY']),
        'stale_assets': len(health_df[health_df['health_status'] == 'STALE']),
        'never_run_assets': len(health_df[health_df['health_status'] == 'NEVER_RUN']),
        'report_time': datetime.now().isoformat(),
        'details': health_df.to_dict('records')
      }
    except Exception as e:
      logger.error(f"Error generating health report: {e}")
      return {
        'total_assets': 0,
        'healthy_assets': 0,
        'unhealthy_assets': 0,
        'stale_assets': 0,
        'never_run_assets': 0,
        'report_time': datetime.now().isoformat(),
        'details': [],
        'error': str(e)
      }
