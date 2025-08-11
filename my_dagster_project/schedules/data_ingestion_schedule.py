from dagster import ScheduleDefinition
from my_dagster_project.jobs.data_ingestion_job import data_ingestion_job
# from datetime import datetime, timedelta

# now = datetime.utcnow() + timedelta(minutes=2)
# cron = f"{now.minute} {now.hour} {now.day} {now.month} *"

data_ingestion_schedule = ScheduleDefinition(
  job=data_ingestion_job,
  execution_timezone='Asia/Ulaanbaatar',

  cron_schedule='0 3 * * *', # Every day at 3:00AM
  name='daily_data_ingestion_schedule'
)
