"""Discover and generate assets"""
from my_dagster_project.core.asset_discovery import AssetDiscovery

discovery = AssetDiscovery()

# Discover all staging tables
print("Discovering staging tables...")
assets = discovery.discover_tables(schema_filter='stg')

print(f"Found {len(assets)} tables")
for asset in assets:
  print(f"  - {asset['asset_key']}")

# Generate Dagster code
if assets:
  discovery.generate_dagster_assets(assets)
  print(f"Generated Dagster assets in assets/auto_generated/")
