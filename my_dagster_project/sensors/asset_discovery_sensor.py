from dagster import sensor, SensorEvaluationContext, RunRequest, SkipReason
from my_dagster_project.core.asset_discovery import asset_discovery
from my_dagster_project.core.metadata_store import metadata_store
import logging

logger = logging.getLogger(__name__)

@sensor(
    name="asset_discovery_sensor",
    minimum_interval_seconds=300,  # Run every 5 minutes
    description="Automatically discovers and registers new assets"
)
def asset_discovery_sensor(context: SensorEvaluationContext):
    """Sensor that automatically discovers and registers new assets"""
    try:
        # Check if discovery should run
        if not asset_discovery.should_run_discovery():
            return SkipReason("Discovery interval not yet reached")
        
        # Discover assets
        discovered_assets = asset_discovery.discover_assets()
        
        if not discovered_assets:
            return SkipReason("No assets discovered")
        
        # Register discovered assets
        registered_count = 0
        for asset_info in discovered_assets:
            try:
                # Register asset with metadata store
                metadata_store.register_asset(
                    asset_key=asset_info["asset_key"],
                    asset_name=asset_info["asset_name"],
                    asset_type=asset_info.get("tags", {}).get("type", "unknown"),
                    group_name=asset_info["group_name"],
                    pipeline_name=asset_info.get("tags", {}).get("pipeline", "default"),
                    owners=asset_info["owners"],
                    tags=asset_info["tags"],
                    metadata=asset_info["metadata"],
                    dependencies=asset_info.get("dependencies", [])
                )
                registered_count += 1
            except Exception as e:
                logger.warning(f"Failed to register asset {asset_info['asset_key']}: {e}")
        
        # Also update lineage information
        lineage = asset_discovery.generate_asset_lineage()
        lineage_count = 0
        for downstream_key, upstream_keys in lineage.items():
            for upstream_key in upstream_keys:
                try:
                    # Add lineage relationship
                    metadata_store.add_lineage_relationship(upstream_key, downstream_key)
                    lineage_count += 1
                except Exception as e:
                    logger.warning(f"Failed to register lineage {upstream_key} -> {downstream_key}: {e}")
        
        logger.info(f"Discovered and registered {registered_count} assets with {lineage_count} lineage relationships")
        return SkipReason(f"Discovered and registered {registered_count} assets with {lineage_count} lineage relationships")
        
    except Exception as e:
        logger.error(f"Asset discovery failed: {e}")
        return SkipReason(f"Asset discovery failed: {e}")