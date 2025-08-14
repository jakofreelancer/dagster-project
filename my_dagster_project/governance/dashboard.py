import pandas as pd
from typing import Dict, List
from datetime import datetime, timedelta

# Use absolute imports instead of relative imports
from my_dagster_project.core.metadata_store import metadata_store
from my_dagster_project.core.monitoring import monitor

class GovernanceDashboard:
    """Centralized dashboard for asset governance and monitoring"""
    
    def get_asset_inventory(self) -> pd.DataFrame:
        """Get a comprehensive inventory of all assets"""
        assets = metadata_store.get_all_assets()
        
        if not assets:
            return pd.DataFrame()
        
        # Convert to DataFrame for easy manipulation
        df = pd.DataFrame(assets)
        
        # Add computed columns
        df['owner_count'] = df['owners'].apply(len)
        df['tag_count'] = df['tags'].apply(len)
        df['dependency_count'] = df['dependencies'].apply(lambda x: len(x) if x else 0)
        
        return df
    
    def get_asset_health(self) -> pd.DataFrame:
        """Get health status of all assets"""
        assets = metadata_store.get_all_assets()
        
        if not assets:
            return pd.DataFrame()
        
        health_data = []
        
        for asset in assets:
            # Get recent executions
            executions = metadata_store.get_asset_executions(asset['asset_key'], limit=5)
            
            # Determine health status
            if not executions:
                health_status = 'UNKNOWN'
            else:
                latest_execution = executions[0]
                if latest_execution['status'] == 'success':
                    # Check if it's stale (no execution in last 24 hours for daily assets)
                    last_run = pd.to_datetime(latest_execution['completed_at'])
                    if datetime.utcnow() - last_run.replace(tzinfo=None) > timedelta(hours=25):
                        health_status = 'STALE'
                    else:
                        health_status = 'HEALTHY'
                else:
                    health_status = 'UNHEALTHY'
            
            # Get active alerts
            alerts = monitor.get_active_alerts(asset['asset_key'])
            alert_count = len(alerts)
            
            health_data.append({
                'asset_key': asset['asset_key'],
                'asset_name': asset['asset_name'],
                'asset_type': asset['asset_type'],
                'group_name': asset['group_name'],
                'health_status': health_status,
                'alert_count': alert_count,
                'last_execution_status': executions[0]['status'] if executions else 'NONE',
                'last_execution_time': executions[0]['completed_at'] if executions else None
            })
        
        return pd.DataFrame(health_data)
    
    def get_pipeline_performance(self) -> pd.DataFrame:
        """Get performance metrics for pipelines"""
        # This would aggregate metrics from the monitoring system
        # For now, we'll return an empty DataFrame as a placeholder
        return pd.DataFrame()
    
    def get_data_quality_report(self) -> pd.DataFrame:
        """Get data quality metrics for assets"""
        # This would analyze data quality metrics
        # For now, we'll return an empty DataFrame as a placeholder
        return pd.DataFrame()
    
    def get_ownership_report(self) -> pd.DataFrame:
        """Get asset ownership report"""
        assets = metadata_store.get_all_assets()
        
        if not assets:
            return pd.DataFrame()
        
        ownership_data = []
        
        for asset in assets:
            for owner in asset['owners']:
                ownership_data.append({
                    'owner': owner,
                    'asset_key': asset['asset_key'],
                    'asset_name': asset['asset_name'],
                    'asset_type': asset['asset_type'],
                    'group_name': asset['group_name']
                })
        
        return pd.DataFrame(ownership_data)
    
    def get_sla_compliance(self) -> pd.DataFrame:
        """Get SLA compliance report"""
        # This would query the monitoring system for SLA data
        # For now, we'll return an empty DataFrame as a placeholder
        return pd.DataFrame()

# Global instance
dashboard = GovernanceDashboard()