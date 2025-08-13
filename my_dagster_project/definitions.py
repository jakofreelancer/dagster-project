from dagster import Definitions

# Import your existing assets
from my_dagster_project.assets import all_assets as existing_assets
from my_dagster_project.jobs import all_jobs
from my_dagster_project.schedules import all_schedules

# Try to import enterprise features (optional)
sensors = []
resources = {}

try:
    from dagster import sensor, RunRequest, SkipReason
    from my_dagster_project.core.asset_discovery import AssetDiscovery
    from my_dagster_project.core.monitoring import monitor
    from my_dagster_project.governance.dashboard import GovernanceDashboard
    
    # Create monitoring sensors only if modules are available
    @sensor(
        minimum_interval_seconds=300,
        default_status=False  # Start as disabled
    )
    def health_monitoring_sensor(context):
        """Monitor asset health and alert on issues"""
        try:
            dashboard = GovernanceDashboard()
            health_df = dashboard.get_pipeline_health()
            
            if not health_df.empty:
                unhealthy = health_df[health_df['health_status'].isin(['UNHEALTHY', 'STALE'])]
                
                if not unhealthy.empty:
                    context.log.warning(f"Found {len(unhealthy)} unhealthy assets")
        except Exception as e:
            context.log.error(f"Health monitoring error: {e}")
        
        return SkipReason("Health check completed")
    
    sensors.append(health_monitoring_sensor)
    resources["dashboard"] = GovernanceDashboard()
    
    # Try to import auto-generated assets
    try:
        from my_dagster_project.assets.auto_generated import all_assets as auto_assets
        combined_assets = [*existing_assets, *auto_assets]
    except ImportError:
        combined_assets = existing_assets
        
except ImportError:
    # Enterprise features not available
    combined_assets = existing_assets

# Create Dagster definitions
defs = Definitions(
    assets=combined_assets,
    jobs=all_jobs,
    schedules=all_schedules,
    sensors=sensors if sensors else None,
    resources=resources if resources else None
)
