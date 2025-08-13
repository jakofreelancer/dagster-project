"""View recent logs"""
from datetime import datetime
from pathlib import Path

today = datetime.now().strftime('%Y_%m_%d')

# Show recent errors
error_log = Path(f'logs/error/error_{today}.log')
if error_log.exists():
  print("Recent Errors:")
  with open(error_log) as f:
    lines = f.readlines()
    for line in lines[-10:]:  # Last 10 errors
      print(line.strip())
else:
  print("No errors today")

# Show recent info
info_log = Path(f'logs/info/app_{today}.log')
if info_log.exists():
  print("\nRecent Activity:")
  with open(info_log) as f:
    lines = f.readlines()
    for line in lines[-20:]:  # Last 20 info messages
      print(line.strip())
