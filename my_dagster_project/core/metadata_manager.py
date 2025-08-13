import json
from typing import Dict, Any, List, Optional
from datetime import datetime
import pyodbc

from my_dagster_project.core.db_config import db_config
from my_dagster_project.core.logging_config import get_logger

logger = get_logger(__name__)

class MetadataManager:
  """Manages asset metadata and registry"""
  
  def __init__(self):
    self.db = db_config
  
  def register_asset(
    self,
    asset_key: str,
    asset_name: str,
    asset_type: str,
    group_name: str = None,
    pipeline_name: str = None,
    owners: List[str] = None,
    tags: Dict[str, str] = None,
    metadata: Dict[str, Any] = None,
    dependencies: List[str] = None,
    **kwargs
  ):
    """Register or update an asset in the registry"""
    
    with self.db.get_connection('metadata_db') as conn:
      cursor = conn.cursor()
      
      # Check if asset exists
      cursor.execute("""
        SELECT asset_id FROM asset_registry 
        WHERE asset_key = ?
      """, asset_key)
      
      existing = cursor.fetchone()
      
      if existing:
        # Update existing asset
        cursor.execute("""
          UPDATE asset_registry SET
            asset_name = ?,
            asset_type = ?,
            group_name = ?,
            pipeline_name = ?,
            owners = ?,
            tags = ?,
            metadata = ?,
            dependencies = ?,
            modified_date = GETDATE()
          WHERE asset_key = ?
        """,
          asset_name,
          asset_type,
          group_name,
          pipeline_name,
          json.dumps(owners) if owners else None,
          json.dumps(tags) if tags else None,
          json.dumps(metadata) if metadata else None,
          json.dumps(dependencies) if dependencies else None,
          asset_key
        )
      else:
        # Insert new asset
        cursor.execute("""
          INSERT INTO asset_registry (
            asset_key, asset_name, asset_type, group_name,
            pipeline_name, owners, tags, metadata,
            dependencies, auto_discovered
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
        """,
          asset_key,
          asset_name,
          asset_type,
          group_name,
          pipeline_name,
          json.dumps(owners) if owners else None,
          json.dumps(tags) if tags else None,
          json.dumps(metadata) if metadata else None,
          json.dumps(dependencies) if dependencies else None
        )
      
      conn.commit()
      logger.info(f"Registered asset: {asset_key}")
  
  def save_asset_execution(
    self,
    asset_key: str,
    run_id: str,
    window_start: datetime = None,
    window_end: datetime = None,
    records_processed: int = 0,
    records_failed: int = 0,
    status: str = 'completed',
    metadata: Dict[str, Any] = None
  ):
    """Save asset execution metadata"""
    
    with self.db.get_connection('metadata_db') as conn:
      cursor = conn.cursor()
      
      cursor.execute("""
        INSERT INTO asset_execution (
          asset_key, run_id, window_start, window_end,
          start_time, end_time, status,
          records_processed, records_failed,
          execution_metadata
        ) VALUES (?, ?, ?, ?, GETDATE(), GETDATE(), ?, ?, ?, ?)
      """,
        asset_key,
        run_id,
        window_start,
        window_end,
        status,
        records_processed,
        records_failed,
        json.dumps(metadata) if metadata else None
      )
      
      conn.commit()
      logger.info(f"Saved execution for {asset_key}")
  
  def get_asset_metadata(self, asset_key: str) -> Dict[str, Any]:
    """Get metadata for an asset"""
    
    with self.db.get_connection('metadata_db') as conn:
      cursor = conn.cursor()
      
      cursor.execute("""
        SELECT 
          metadata,
          config,
          dependencies,
          owners,
          tags
        FROM asset_registry
        WHERE asset_key = ? AND is_active = 1
      """, asset_key)
      
      row = cursor.fetchone()
      
      if row:
        return {
          'metadata': json.loads(row[0] or '{}'),
          'config': json.loads(row[1] or '{}'),
          'dependencies': json.loads(row[2] or '[]'),
          'owners': json.loads(row[3] or '[]'),
          'tags': json.loads(row[4] or '{}')
        }
      
      return {}

# Global instance
metadata_manager = MetadataManager()
