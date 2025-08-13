import logging
import logging.config
import yaml
import os
from pathlib import Path
from datetime import datetime

class CentralizedLogger:
  """Centralized logging with daily rotation"""
  
  _instance = None
  _initialized = False
  
  def __new__(cls):
    if cls._instance is None:
      cls._instance = super().__new__(cls)
    return cls._instance
  
  def __init__(self):
    if not self._initialized:
      self._setup_logging()
      self._initialized = True
  
  def _setup_logging(self):
    """Initialize logging configuration"""
    # Create log directories
    log_base = Path('logs')
    for subdir in ['info', 'error', 'debug']:
      (log_base / subdir).mkdir(parents=True, exist_ok=True)
    
    # Load logging config
    config_path = Path(__file__).parent.parent / 'config' / 'logging.yaml'
    
    with open(config_path, 'r') as f:
      config = yaml.safe_load(f)
    
    # Update filenames with date
    today = datetime.now().strftime('%Y_%m_%d')
    
    for handler_name, handler in config['handlers'].items():
      if 'filename' in handler and handler_name != 'debug_file':
        base_path = Path(handler['filename'])
        handler['filename'] = str(
          base_path.parent / f"{base_path.stem}_{today}{base_path.suffix}"
        )
    
    logging.config.dictConfig(config)
  
  @staticmethod
  def get_logger(name: str) -> logging.Logger:
    """Get logger for specific module"""
    return logging.getLogger(name)
  
  @staticmethod
  def log_execution(func):
    """Decorator for logging function execution"""
    logger = logging.getLogger(func.__module__)
    
    def wrapper(*args, **kwargs):
      logger.info(f"Starting {func.__name__}")
      try:
        result = func(*args, **kwargs)
        logger.info(f"Completed {func.__name__}")
        return result
      except Exception as e:
        logger.error(f"Error in {func.__name__}: {str(e)}", exc_info=True)
        raise
    
    return wrapper

# Initialize logging
central_logger = CentralizedLogger()
get_logger = central_logger.get_logger
log_execution = central_logger.log_execution
