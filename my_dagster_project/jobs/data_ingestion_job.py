from dagster import define_asset_job

data_ingestion_job = define_asset_job(
  name='data_ingestion_job',
  selection=[
    'raw_blast_shotcrete_71_data',
    'stg_blast_shotcrete_71_data',
  ]
)
