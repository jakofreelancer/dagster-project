import yaml
import os
import pyodbc
from typing import Dict, Any, Optional
from pathlib import Path

class DatabaseConfig:
  """Enhanced database configuration with environment support"""
  
  def __init__(self, environment: str = None):
    self.environment = environment or os.getenv('ENVIRONMENT', 'dev')
    self.config = self._load_config()
    self._initialize_metadata_database()
  
  def _load_config(self) -> Dict:
    """Load database configuration for current environment"""
    config_path = Path(__file__).parent.parent / 'config' / 'database.yaml'
    
    with open(config_path, 'r') as f:
      all_config = yaml.safe_load(f)
    
    return all_config['environments'][self.environment]
  
  def _initialize_metadata_database(self):
    """Create metadata tables if they don't exist"""
    with self.get_connection('metadata_db') as conn:
      cursor = conn.cursor()
      
      # Asset registry table
      cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'asset_registry')
        CREATE TABLE asset_registry (
          asset_id INT IDENTITY(1,1) PRIMARY KEY,
          asset_key NVARCHAR(500) NOT NULL UNIQUE,
          asset_name NVARCHAR(255) NOT NULL,
          asset_type NVARCHAR(50) NOT NULL,
          pipeline_name NVARCHAR(255),
          group_name NVARCHAR(255),
          table_name NVARCHAR(255),
          schema_name NVARCHAR(128),
          database_name NVARCHAR(128),
          
          -- Metadata
          description NVARCHAR(MAX),
          owners NVARCHAR(MAX),
          tags NVARCHAR(MAX),
          metadata NVARCHAR(MAX),
          
          -- Configuration
          config NVARCHAR(MAX),
          dependencies NVARCHAR(MAX),
          
          -- Governance
          data_classification NVARCHAR(50),
          contains_pii BIT DEFAULT 0,
          retention_days INT,
          
          -- Auto-discovery
          auto_discovered BIT DEFAULT 0,
          source_query NVARCHAR(MAX),
          
          -- Tracking
          is_active BIT DEFAULT 1,
          created_date DATETIME2 DEFAULT GETDATE(),
          created_by NVARCHAR(255) DEFAULT SYSTEM_USER,
          modified_date DATETIME2 DEFAULT GETDATE(),
          
          INDEX idx_asset_key (asset_key),
          INDEX idx_pipeline (pipeline_name)
        )
      """)
      
      # Asset execution metadata
      cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'asset_execution')
        CREATE TABLE asset_execution (
          execution_id INT IDENTITY(1,1) PRIMARY KEY,
          asset_key NVARCHAR(500) NOT NULL,
          run_id NVARCHAR(255) NOT NULL,
          
          -- Window tracking
          window_start DATETIME2,
          window_end DATETIME2,
          
          -- Execution details
          start_time DATETIME2 NOT NULL,
          end_time DATETIME2,
          status NVARCHAR(50) NOT NULL,
          
          -- Metrics
          records_processed INT,
          records_failed INT,
          duration_seconds INT,
          
          -- Metadata
          execution_metadata NVARCHAR(MAX),
          
          created_date DATETIME2 DEFAULT GETDATE(),
          INDEX idx_asset_execution (asset_key, run_id),
          INDEX idx_window (asset_key, window_start, window_end)
        )
      """)
      
      # Pipeline monitoring
      cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'pipeline_metrics')
        CREATE TABLE pipeline_metrics (
          metric_id INT IDENTITY(1,1) PRIMARY KEY,
          pipeline_name NVARCHAR(255) NOT NULL,
          asset_key NVARCHAR(500),
          metric_name NVARCHAR(255) NOT NULL,
          metric_value FLOAT NOT NULL,
          metric_timestamp DATETIME2 DEFAULT GETDATE(),
          INDEX idx_pipeline_metric (pipeline_name, metric_timestamp DESC)
        )
      """)
      
      conn.commit()
  
  def get_connection(self, db_type: str = 'data_db', source_name: str = None):
    """Get connection to specific database"""
    if db_type == 'source' and source_name:
      config = self.config['source_dbs'][source_name]
    else:
      config = self.config[db_type]
    
    conn_str = self._build_connection_string(config)
    return pyodbc.connect(conn_str)
  
  def _build_connection_string(self, config: Dict) -> str:
    """Build connection string from config"""
    parts = [
      f"Driver={config['driver']}",
      f"Server={config['server']}",
      f"Database={config['database']}"
    ]
    
    if config.get('trusted_connection'):
      parts.append("Trusted_Connection=yes")
    else:
      parts.append(f"UID={config.get('username')}")
      parts.append(f"PWD={config.get('password')}")
    
    return ';'.join(parts)

# Global instance
db_config = DatabaseConfig()
