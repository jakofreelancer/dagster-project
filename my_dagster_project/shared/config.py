def get_config_value(key: str, default=None):
  return CONFIG.get(key.lower(), default)

# Configuration options:
# - 'log_level': Set the logging level ('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'). Default is 'INFO'.
# - 'log_dir': Directory where log files will be stored. Default is 'logs'.
# - 'log_file': Optional override for log file name. If not set, defaults to 'YYYY-MM-DD.log'.

CONFIG = {
  'load_mode': 'full',                   # Options: full, eom, current_month, incremental
  'lock_time': '01 00:00:00',            # Format: 'DD HH:MM:SS'
  'full_mode_start_date': '2024-01-01',  # Only used in full mode
  'incremental_days': '5',               # Optional: used for incremental
  'sql_server': 'MNOYTSQLD40',
  'sql_database': 'UG_Ore_Tracking',
  #'prep_suffix': '_prep',
  'staging_target_schema': 'stg',
  'integration_target_schema': 'intg',
  'reporting_target_schema': 'rpt',
  'archiving_target_schema': 'arch',
  'log_level': 'INFO',
  'log_dir': 'logs'
}
