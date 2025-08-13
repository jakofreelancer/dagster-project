import json
from typing import Dict, List, Any
from datetime import datetime
from pathlib import Path
from textwrap import dedent

from my_dagster_project.core.db_config import db_config
from my_dagster_project.core.metadata_manager import metadata_manager
from my_dagster_project.core.logging_config import get_logger

logger = get_logger(__name__)

class AssetDiscovery:
  """Auto-discover database objects and generate Dagster assets"""
  
  def __init__(self):
    self.db = db_config
  
  def discover_tables(self, database: str = 'data_db', schema_filter: str = None) -> List[Dict]:
    """Discover tables in database"""
    assets = []
    
    with self.db.get_connection(database) as conn:
      cursor = conn.cursor()
      
      query = """
        SELECT 
          s.name as schema_name,
          t.name as table_name
        FROM sys.tables t
        JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE s.name NOT IN ('sys', 'INFORMATION_SCHEMA')
      """
      
      if schema_filter:
        query += f" AND s.name = '{schema_filter}'"
      
      cursor.execute(query)
      
      for row in cursor.fetchall():
        asset_key = f"{row.schema_name}.{row.table_name}"
        
        # Register in metadata
        metadata_manager.register_asset(
          asset_key=asset_key,
          asset_name=row.table_name,
          asset_type='source',
          group_name='auto_discovered',
          metadata={
            'schema': row.schema_name,
            'table': row.table_name,
            'database': database,
            'discovered_at': datetime.now().isoformat()
          }
        )
        
        assets.append({
          'asset_key': asset_key,
          'table_name': row.table_name,
          'schema_name': row.schema_name
        })
    
    logger.info(f"Discovered {len(assets)} tables")
    return assets
  
  def generate_dagster_assets(self, assets: List[Dict], output_dir: str = 'assets/auto_generated'):
    """Generate Dagster asset definitions"""
    
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    code = dedent("""
    # Auto-generated assets
    from dagster import asset, Output, MetadataValue
    import pandas as pd
    from my_dagster_project.core.db_config import db_config
    from my_dagster_project.core.metadata_manager import metadata_manager
    from my_dagster_project.core.logging_config import get_logger
    
    logger = get_logger(__name__)
    
    """)
    
    for asset_info in assets:
      safe_name = asset_info['table_name'].lower().replace('-', '_')
      
      asset_code = dedent(f"""
        @asset(
          name="{safe_name}",
          key_prefix=["{asset_info['schema_name']}"],
          group_name="auto_discovered",
          compute_kind="SQL"
        )
        def {safe_name}(context):
          '''Auto-generated asset for {asset_info['asset_key']}'''
          
          with db_config.get_connection('data_db') as conn:
            query = "SELECT * FROM [{asset_info['schema_name']}].[{asset_info['table_name']}]"
            df = pd.read_sql(query, conn)
          
          context.add_output_metadata({{
            "row_count": len(df),
            "columns": list(df.columns)
          }})
            
          metadata_manager.save_asset_execution(
              asset_key="{asset_info['asset_key']}",
              run_id=context.run_id,
              records_processed=len(df)
          )
          
          return df 
      """)
        
      code += asset_code
    
      code += "\nall_assets = [" + ", ".join([a['table_name'].lower().replace('-', '_') for a in assets]) + "]"
    
    (output_path / 'discovered_assets.py').write_text(code)
    (output_path / '__init__.py').write_text("from .discovered_assets import all_assets")
    
    logger.info(f"Generated {len(assets)} Dagster assets")
