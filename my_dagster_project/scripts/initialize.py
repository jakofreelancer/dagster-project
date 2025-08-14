"""
Initialization script for the enterprise asset management system.
This script sets up the initial assets and configurations.
"""

import logging
from my_dagster_project.core.asset_discovery import asset_discovery
from my_dagster_project.core.metadata_store import metadata_store
from my_dagster_project.core.asset_record import asset_record
from my_dagster_project.core.job_scheduler import job_scheduler

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def initialize_asset_management():
    """Initialize the asset management system"""
    logger.info("Initializing enterprise asset management system...")
    
    try:
        # Run initial asset discovery
        logger.info("Running initial asset discovery...")
        discovered_assets = asset_discovery.discover_assets(force=True)
        logger.info(f"Discovered {len(discovered_assets)} assets")
        
        # Register all discovered assets in the flexible asset record system
        registered_count = asset_discovery.register_discovered_assets()
        logger.info(f"Registered {registered_count} assets in asset record system")
        
        # Also register in metadata store for backward compatibility
        metadata_registered_count = 0
        for asset_info in discovered_assets:
            try:
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
                metadata_registered_count += 1
            except Exception as e:
                logger.warning(f"Failed to register asset {asset_info['asset_key']} in metadata store: {e}")
        
        logger.info(f"Registered {metadata_registered_count} assets in metadata store")
        
        # Set up lineage relationships
        lineage = asset_discovery.generate_asset_lineage()
        lineage_count = 0
        for downstream_key, upstream_keys in lineage.items():
            for upstream_key in upstream_keys:
                try:
                    metadata_store.add_lineage_relationship(upstream_key, downstream_key)
                    lineage_count += 1
                except Exception as e:
                    logger.warning(f"Failed to register lineage {upstream_key} -> {downstream_key}: {e}")
        
        logger.info(f"Set up {lineage_count} lineage relationships")
        
        # Initialize job scheduler with default jobs
        logger.info("Initializing job scheduler...")
        job_scheduler.initialize_default_jobs()
        
        # Start job scheduler if enabled
        job_scheduler.start_scheduler()
        
        logger.info("Asset management system initialized successfully!")
        
    except Exception as e:
        logger.error(f"Failed to initialize asset management system: {e}")
        raise

if __name__ == "__main__":
    initialize_asset_management()