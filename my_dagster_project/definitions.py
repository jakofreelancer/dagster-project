from dagster import Definitions, DefaultSensorStatus

# Import your existing assets
from my_dagster_project.assets import all_assets as existing_assets
from my_dagster_project.jobs import all_jobs
from my_dagster_project.schedules import all_schedules

# Import sensors
from my_dagster_project.sensors.asset_discovery_sensor import asset_discovery_sensor

# Import enterprise features
from my_dagster_project.core.asset_discovery import AssetDiscovery
from my_dagster_project.core.monitoring import monitor
from my_dagster_project.core.metadata_store import metadata_store
from my_dagster_project.governance.dashboard import GovernanceDashboard

# Sensors and resources
sensors = [
    asset_discovery_sensor
]

# Health monitoring sensor
from dagster import sensor, RunRequest, SkipReason

@sensor(
    name="health_monitoring_sensor",
    minimum_interval_seconds=300,
    default_status=DefaultSensorStatus.RUNNING,
    description="Monitor asset health and alert on issues"
)
def health_monitoring_sensor(context):
    """Monitor asset health and alert on issues"""
    try:
        dashboard = GovernanceDashboard()
        health_df = dashboard.get_asset_health()
        
        if not health_df.empty:
            unhealthy = health_df[health_df['health_status'].isin(['UNHEALTHY', 'STALE'])]
            
            if not unhealthy.empty:
                context.log.warning(f"Found {len(unhealthy)} unhealthy assets")
                # In a real implementation, you might want to trigger alerts here
    except Exception as e:
        context.log.error(f"Health monitoring error: {e}")
    
    return SkipReason("Health check completed")

sensors.append(health_monitoring_sensor)

# Try to import auto-generated assets
try:
    from my_dagster_project.assets.auto_generated import all_assets as auto_assets
    combined_assets = [*existing_assets, *auto_assets]
except ImportError:
    combined_assets = existing_assets

# Resources
resources = {
    "metadata_store": metadata_store,
    "monitor": monitor,
    "dashboard": GovernanceDashboard()
}

# Initialize asset discovery
asset_discovery = AssetDiscovery()

# Create Dagster definitions
defs = Definitions(
    assets=combined_assets,
    jobs=all_jobs,
    schedules=all_schedules,
    sensors=sensors,
    resources=resources
)
