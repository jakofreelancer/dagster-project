from dagster import AssetExecutionContext, asset
from my_dagster_project.shared.load_to_sql_utils import load_to_stg_table, sanitize_dataframe
from pandas import DataFrame

@asset(
  name="stg_blast_shotcrete_71_data",
  group_name="stg__loaders",
  description="Raw blast shotcrete data for SQL staging, filtered by latest process per BlastID+Source.",
  compute_kind="pandas",
  owners=["javkhlanbu@ot.mn", "myagmarsurens@ot.mn"],
  tags={"domain": "blast", "source": "MineSys"},
  type="raw",
  metadata={
    "source_system": "MineSys SQL71",
    "target_schema": "stg",
    "load_window": "dynamic via get_load_window()",
    "owner": "javkhlanbu",
    "team": "Geology",
    "type": "raw",
    "schedule": "daily"
  },
  code_version="v1.2"
)
def stg_blast_shotcrete_71_data(
  context: AssetExecutionContext,
  raw_blast_shotcrete_71_data: DataFrame
):
  '''
    Loads the raw blast shotcrete data into a preparation table for further processing.
    The data is sanitized to ensure compatibility with SQL Server.
  '''

  sanitized_data = sanitize_dataframe(raw_blast_shotcrete_71_data)

  load_to_stg_table(
    context=context,
    df=sanitized_data,
    target_table_name='sql71_blast_dagster_test'
  )

all_assets = [stg_blast_shotcrete_71_data]
