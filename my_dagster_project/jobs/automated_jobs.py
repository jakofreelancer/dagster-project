from my_dagster_project.core.health_monitor import health_monitor
from my_dagster_project.core.asset_record import asset_record
from my_dagster_project.core.metadata_store import metadata_store
import logging

logger = logging.getLogger(__name__)

def run_health_checks(config: dict = None) -> dict:
    """Run health checks for all assets"""
    try:
        # Get all assets
        assets = asset_record.get_all_assets()
        
        if not assets:
            logger.info("No assets found for health checks")
            return {"status": "completed", "assets_checked": 0}
        
        # Run health checks for each asset
        results = []
        for asset in assets:
            try:
                # Get execution history for this asset
                execution_history = metadata_store.get_asset_executions(asset['asset_key'], limit=10)
                
                # Run health checks
                health_result = health_monitor.run_health_checks(
                    asset_key=asset['asset_key'],
                    execution_history=execution_history,
                    metadata=asset.get('metadata', {})
                )
                
                results.append(health_result)
                
                # If asset is unhealthy, create an alert
                if health_result['overall_status'] == 'UNHEALTHY':
                    asset_record.create_asset_alert(
                        asset_key=asset['asset_key'],
                        alert_type='HEALTH_CHECK_FAILED',
                        severity='HIGH',
                        message=f'Asset health check failed: {health_result.get("checks", [{}])[0].get("message", "Unknown issue")}'
                    )
                    
            except Exception as e:
                logger.error(f"Failed to run health check for asset {asset['asset_key']}: {e}")
                # Create alert for failed health check
                asset_record.create_asset_alert(
                    asset_key=asset['asset_key'],
                    alert_type='HEALTH_CHECK_ERROR',
                    severity='MEDIUM',
                    message=f'Health check execution failed: {str(e)}'
                )
        
        logger.info(f"Completed health checks for {len(results)} assets")
        return {
            "status": "completed",
            "assets_checked": len(results),
            "results": results
        }
        
    except Exception as e:
        logger.error(f"Health check job failed: {e}")
        return {
            "status": "failed",
            "error": str(e)
        }

def run_asset_discovery(config: dict = None) -> dict:
    """Run asset discovery and registration"""
    try:
        from my_dagster_project.core.asset_discovery import asset_discovery
        
        # Run discovery
        discovered_assets = asset_discovery.discover_assets()
        
        # Register discovered assets
        registered_count = asset_discovery.register_discovered_assets()
        
        logger.info(f"Discovered {len(discovered_assets)} assets, registered {registered_count}")
        return {
            "status": "completed",
            "assets_discovered": len(discovered_assets),
            "assets_registered": registered_count
        }
        
    except Exception as e:
        logger.error(f"Asset discovery job failed: {e}")
        return {
            "status": "failed",
            "error": str(e)
        }

def process_alerts(config: dict = None) -> dict:
    """Process and notify on active alerts"""
    try:
        # Get all active alerts
        alerts = asset_record.get_active_alerts()
        
        if not alerts:
            logger.info("No active alerts to process")
            return {"status": "completed", "alerts_processed": 0}
        
        # In a real implementation, you would:
        # 1. Group alerts by severity
        # 2. Send notifications (email, Slack, etc.)
        # 3. Escalate based on severity and age
        # 4. Update alert status
        
        # For now, we'll just log them
        high_severity_alerts = [a for a in alerts if a['severity'] == 'HIGH']
        medium_severity_alerts = [a for a in alerts if a['severity'] == 'MEDIUM']
        low_severity_alerts = [a for a in alerts if a['severity'] == 'LOW']
        
        logger.warning(f"Processing alerts - High: {len(high_severity_alerts)}, "
                      f"Medium: {len(medium_severity_alerts)}, Low: {len(low_severity_alerts)}")
        
        # Example notification logic (simplified)
        if high_severity_alerts:
            logger.error(f"High severity alerts detected: {len(high_severity_alerts)}")
            # In real implementation: send immediate notification
            
        return {
            "status": "completed",
            "alerts_processed": len(alerts),
            "high_severity": len(high_severity_alerts),
            "medium_severity": len(medium_severity_alerts),
            "low_severity": len(low_severity_alerts)
        }
        
    except Exception as e:
        logger.error(f"Alert processing job failed: {e}")
        return {
            "status": "failed",
            "error": str(e)
        }