import logging
import os
from datetime import datetime
from .config import get_config_value

def setup_logging(logger_name: str = "my_app") -> logging.Logger:
  """
    Sets up structured logging with both file and console outputs.
    
    Config options from config.py:
    - log_level (default "INFO")
    - log_dir (default "logs/")
    - log_file (optional override file name)

    Args:
      logger_name (str): Name of the logger (default = "my_app")

    Returns:
      logging.Logger: Configured logger instance
  """
  log_level = get_config_value("log_level", "INFO").upper()
  log_dir = get_config_value("log_dir", "logs")
  log_file_override = get_config_value("log_file", None)

  os.makedirs(log_dir, exist_ok=True)

  if log_file_override:
    log_filename = log_file_override
  else:
    log_filename = f"{datetime.now().strftime('%Y-%m-%d')}.log"

  log_path = os.path.join(log_dir, log_filename)

  logger = logging.getLogger(logger_name)
  logger.setLevel(getattr(logging, log_level, logging.INFO))

  # Avoid adding handlers multiple times
  if logger.hasHandlers():
    return logger

  formatter = logging.Formatter(
    "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
  )

  console_handler = logging.StreamHandler()
  console_handler.setFormatter(formatter)
  logger.addHandler(console_handler)

  file_handler = logging.FileHandler(log_path)
  file_handler.setFormatter(formatter)
  logger.addHandler(file_handler)

  return logger
