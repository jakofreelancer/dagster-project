from datetime import datetime, timedelta, time
from .config import get_config_value


def parse_lock_time_config(raw_val: str) -> tuple[int, time]:
  """
    Parses a config string like '01 00:00:00' into a (day, time) tuple.
  """
  day = int(raw_val[:2].strip())
  h, m, s = map(int, raw_val[3:].strip().split(':'))
  return day, time(hour=h, minute=m, second=s)


def apply_lock_time(base_date: datetime, lock_time: time) -> datetime:
  """
    Combines a date and a lock time into a datetime.
  """
  return datetime.combine(base_date.date(), lock_time)


def get_next_month_start(date: datetime) -> datetime:
  """
    Returns the first day of the next month.
  """
  return (date + timedelta(days=32)).replace(day=1)


def get_load_window() -> tuple[datetime, datetime]:
  """
    Calculates the start and end of the data load window based on config:
    - `load_mode`: full, eom, or current_month
    - `lock_time`: string like '01 00:00:00'
    - `full_mode_start_date`: used when mode = full
  """
  now = datetime.now()
  current_month_start = now.replace(day=1)

  lock_raw = get_config_value('lock_time', '01 00:00:00')
  load_mode = get_config_value('load_mode', 'current_month').strip().lower()
  full_mode_start = get_config_value('full_mode_start_date', '2024-01-01')

  lock_day, lock_time = parse_lock_time_config(lock_raw)

  if load_mode == 'full':
    start = apply_lock_time(datetime.strptime(full_mode_start, '%Y-%m-%d'), lock_time)
  elif load_mode == 'eom':
    prev_month_start = (current_month_start - timedelta(days=1)).replace(day=1)
    start = apply_lock_time(prev_month_start, lock_time)
  elif load_mode == 'current_month':
    start = apply_lock_time(current_month_start, lock_time)
  else:
    raise ValueError(f'Invalid load_mode: {load_mode}')

  end = apply_lock_time(get_next_month_start(start), lock_time)
  return start, end
