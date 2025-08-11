from dagster import Definitions
from my_dagster_project.assets import all_assets
from my_dagster_project.jobs import all_jobs
from my_dagster_project.schedules import all_schedules

defs = Definitions(
    assets=all_assets,          # assets must have @asset(group_name="...")
    jobs=all_jobs,
    schedules=all_schedules,
)
