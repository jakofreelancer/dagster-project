import os

def get_config_value(key: str, default=None):
  """Get configuration value with environment override"""
  # Check environment variable first
  env_key = f"DAGSTER_{key.upper()}"
  if env_key in os.environ:
    return os.environ[env_key]
  
  return CONFIG.get(key.lower(), default)

# Enhanced configuration with environment support
CONFIG = {
  'environment': os.getenv('ENVIRONMENT', 'dev'),
  'load_mode': 'full',
  'lock_time': '01 00:00:00',
  'full_mode_start_date': '2024-01-01',
  'incremental_days': '5',
  'sql_server': 'MNOYTSQLD40',
  'sql_database': 'UG_Ore_Tracking',
  'staging_target_schema': 'stg',
  'integration_target_schema': 'intg',
  'reporting_target_schema': 'rpt',
  'archiving_target_schema': 'arch',
  'log_level': 'INFO',
  'log_dir': 'logs',
  
  # Monitoring settings
  'enable_monitoring': True,
  'enable_metadata_tracking': True,
  'health_check_interval': 300,
  
  # Auto-discovery settings
  'auto_discover_enabled': True,
  'auto_discover_schemas': ['stg', 'intg', 'rpt']
}
