from dagster import Definitions, sensor, RunRequest, SkipReason
from datetime import datetime

# Import your existing assets
from my_dagster_project.assets import all_assets as existing_assets
from my_dagster_project.jobs import all_jobs
from my_dagster_project.schedules import all_schedules

# Import enterprise features
from my_dagster_project.core.asset_discovery import AssetDiscovery
from my_dagster_project.core.monitoring import monitor
from my_dagster_project.governance.dashboard import GovernanceDashboard

# Try to import auto-generated assets
try:
    from my_dagster_project.assets.auto_generated import all_assets as auto_assets
    combined_assets = [*existing_assets, *auto_assets]
except ImportError:
    combined_assets = existing_assets

# Create monitoring sensors
@sensor(
    minimum_interval_seconds=300,  # Every 5 minutes
    default_status=True
)
def health_monitoring_sensor(context):
    """Monitor asset health and alert on issues"""
    
    dashboard = GovernanceDashboard()
    health_df = dashboard.get_pipeline_health()
    
    unhealthy = health_df[health_df['health_status'].isin(['UNHEALTHY', 'STALE'])]
    
    if not unhealthy.empty:
        context.log.warning(f"Found {len(unhealthy)} unhealthy assets")
        
        # Could trigger an alert job here
        for _, asset in unhealthy.iterrows():
            context.log.warning(
                f"Asset {asset['asset_key']} is {asset['health_status']}"
            )
    
    return SkipReason("Health check completed")

@sensor(
    minimum_interval_seconds=3600,  # Every hour
    default_status=False  # Enable manually when needed
)
def asset_discovery_sensor(context):
    """Discover new database objects"""
    
    discovery = AssetDiscovery()
    
    # Discover tables in staging schema
    new_assets = discovery.discover_tables(schema_filter='stg')
    
    if new_assets:
        discovery.generate_dagster_assets(new_assets)
        
        return RunRequest(
            run_config={},
            tags={"discovered_assets": str(len(new_assets))}
        )
    
    return SkipReason("No new assets discovered")

# Create Dagster definitions
defs = Definitions(
    assets=combined_assets,
    jobs=all_jobs,
    schedules=all_schedules,
    sensors=[
        health_monitoring_sensor,
        asset_discovery_sensor
    ],
    resources={
        "dashboard": GovernanceDashboard()
    }
)

# Initialize on startup
if __name__ == "__main__":
    from my_dagster_project.core.logging_config import get_logger
    
    logger = get_logger(__name__)
    logger.info("Initializing Dagster with enterprise features")
    
    # Run initial discovery if enabled
    from my_dagster_project.shared.config import get_config_value
    
    if get_config_value('auto_discover_enabled'):
        discovery = AssetDiscovery()
        assets = discovery.discover_tables(schema_filter='stg')
        
        if assets:
            discovery.generate_dagster_assets(assets)
            logger.info(f"Discovered and generated {len(assets)} assets")
