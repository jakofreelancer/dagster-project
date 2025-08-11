from my_dagster_project.assets.ingestion.stg_blast_shotcrete_71_ingestion import all_assets as ingest_assets
from my_dagster_project.assets.loaders.stg_blast_shotcrete_71_loader import all_assets as load_assets

all_assets = [
  *ingest_assets,
  *load_assets,
]
