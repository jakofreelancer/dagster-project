import sqlite3
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from contextlib import contextmanager
from enum import Enum

class HealthStatus(Enum):
    HEALTHY = "HEALTHY"
    UNHEALTHY = "UNHEALTHY"
    STALE = "STALE"
    UNKNOWN = "UNKNOWN"

class HealthCheckType(Enum):
    EXECUTION_STATUS = "execution_status"
    DATA_VOLUME = "data_volume"
    EXECUTION_TIME = "execution_time"
    DATA_QUALITY = "data_quality"
    DEPENDENCY = "dependency"

class HealthMonitor:
    """Enhanced health monitoring system with automated checks"""
    
    def __init__(self, db_path: str = "health.db"):
        self.db_path = db_path
        self.init_db()
    
    @contextmanager
    def get_connection(self):
        """Context manager for database connections"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()
    
    def init_db(self):
        """Initialize the health monitoring database"""
        with self.get_connection() as conn:
            # Health checks table
            conn.execute('''
                CREATE TABLE IF NOT EXISTS health_checks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    asset_key TEXT,
                    check_type TEXT,
                    status TEXT,
                    message TEXT,
                    details TEXT, -- JSON serialized dict
                    checked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (asset_key) REFERENCES assets (asset_key)
                )
            ''')
            
            # Health summary table
            conn.execute('''
                CREATE TABLE IF NOT EXISTS health_summary (
                    asset_key TEXT PRIMARY KEY,
                    overall_status TEXT,
                    last_healthy TIMESTAMP,
                    failure_count INTEGER DEFAULT 0,
                    last_checked TIMESTAMP,
                    FOREIGN KEY (asset_key) REFERENCES assets (asset_key)
                )
            ''')
            
            conn.commit()
    
    def run_health_checks(self, asset_key: str, execution_history: List[Dict], metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Run all relevant health checks for an asset"""
        checks_results = []
        
        # 1. Execution status check
        exec_check = self._check_execution_status(execution_history)
        checks_results.append(exec_check)
        
        # 2. Data volume check (if applicable)
        volume_check = self._check_data_volume(execution_history, metadata)
        checks_results.append(volume_check)
        
        # 3. Execution time check (if applicable)
        time_check = self._check_execution_time(execution_history, metadata)
        checks_results.append(time_check)
        
        # Save check results
        for check in checks_results:
            self._save_health_check(asset_key, check)
        
        # Calculate overall health status
        overall_status = self._calculate_overall_health(checks_results)
        
        # Update health summary
        self._update_health_summary(asset_key, overall_status, checks_results)
        
        return {
            "asset_key": asset_key,
            "overall_status": overall_status.value,
            "checks": checks_results,
            "timestamp": datetime.utcnow()
        }
    
    def _check_execution_status(self, execution_history: List[Dict]) -> Dict[str, Any]:
        """Check the execution status of recent runs"""
        if not execution_history:
            return {
                "type": HealthCheckType.EXECUTION_STATUS.value,
                "status": HealthStatus.UNKNOWN.value,
                "message": "No execution history available",
                "details": {}
            }
        
        latest_execution = execution_history[0]
        if latest_execution['status'] == 'success':
            return {
                "type": HealthCheckType.EXECUTION_STATUS.value,
                "status": HealthStatus.HEALTHY.value,
                "message": "Latest execution successful",
                "details": {"run_id": latest_execution['run_id']}
            }
        else:
            return {
                "type": HealthCheckType.EXECUTION_STATUS.value,
                "status": HealthStatus.UNHEALTHY.value,
                "message": f"Latest execution failed: {latest_execution.get('metadata', {}).get('error', 'Unknown error')}",
                "details": {"run_id": latest_execution['run_id']}
            }
    
    def _check_data_volume(self, execution_history: List[Dict], metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Check if data volume is within expected range"""
        if not execution_history or len(execution_history) < 2:
            return {
                "type": HealthCheckType.DATA_VOLUME.value,
                "status": HealthStatus.UNKNOWN.value,
                "message": "Insufficient execution history for volume check",
                "details": {}
            }
        
        # Get recent records processed counts
        recent_volumes = [
            exec_data['records_processed'] 
            for exec_data in execution_history[:5] 
            if exec_data.get('records_processed') is not None
        ]
        
        if len(recent_volumes) < 2:
            return {
                "type": HealthCheckType.DATA_VOLUME.value,
                "status": HealthStatus.UNKNOWN.value,
                "message": "Insufficient volume data for comparison",
                "details": {}
            }
        
        current_volume = recent_volumes[0]
        previous_volume = recent_volumes[1]
        
        # Calculate percentage change
        if previous_volume > 0:
            volume_change = abs(current_volume - previous_volume) / previous_volume
        else:
            volume_change = 1.0 if current_volume > 0 else 0.0
        
        # Check against threshold (configurable, default 20%)
        threshold = metadata.get('health_config', {}).get('volume_threshold', 0.2)
        
        if volume_change > threshold:
            return {
                "type": HealthCheckType.DATA_VOLUME.value,
                "status": HealthStatus.UNHEALTHY.value,
                "message": f"Data volume changed by {volume_change:.2%}, exceeds threshold of {threshold:.2%}",
                "details": {
                    "current_volume": current_volume,
                    "previous_volume": previous_volume,
                    "change_percentage": volume_change
                }
            }
        else:
            return {
                "type": HealthCheckType.DATA_VOLUME.value,
                "status": HealthStatus.HEALTHY.value,
                "message": f"Data volume stable (change: {volume_change:.2%})",
                "details": {
                    "current_volume": current_volume,
                    "previous_volume": previous_volume,
                    "change_percentage": volume_change
                }
            }
    
    def _check_execution_time(self, execution_history: List[Dict], metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Check if execution time is within expected range"""
        if not execution_history or len(execution_history) < 2:
            return {
                "type": HealthCheckType.EXECUTION_TIME.value,
                "status": HealthStatus.UNKNOWN.value,
                "message": "Insufficient execution history for time check",
                "details": {}
            }
        
        # Get recent execution times
        recent_times = []
        for exec_data in execution_history[:5]:
            if exec_data.get('started_at') and exec_data.get('completed_at'):
                start = datetime.fromisoformat(exec_data['started_at'].replace('Z', '+00:00'))
                end = datetime.fromisoformat(exec_data['completed_at'].replace('Z', '+00:00'))
                duration = (end - start).total_seconds()
                recent_times.append(duration)
        
        if len(recent_times) < 2:
            return {
                "type": HealthCheckType.EXECUTION_TIME.value,
                "status": HealthStatus.UNKNOWN.value,
                "message": "Insufficient timing data for comparison",
                "details": {}
            }
        
        current_time = recent_times[0]
        avg_time = sum(recent_times[1:]) / len(recent_times[1:])
        
        # Calculate percentage change
        if avg_time > 0:
            time_change = abs(current_time - avg_time) / avg_time
        else:
            time_change = 1.0 if current_time > 0 else 0.0
        
        # Check against threshold (configurable, default 50%)
        threshold = metadata.get('health_config', {}).get('time_threshold', 0.5)
        
        if time_change > threshold:
            return {
                "type": HealthCheckType.EXECUTION_TIME.value,
                "status": HealthStatus.UNHEALTHY.value,
                "message": f"Execution time changed by {time_change:.2%}, exceeds threshold of {threshold:.2%}",
                "details": {
                    "current_time": current_time,
                    "average_time": avg_time,
                    "change_percentage": time_change
                }
            }
        else:
            return {
                "type": HealthCheckType.EXECUTION_TIME.value,
                "status": HealthStatus.HEALTHY.value,
                "message": f"Execution time stable (change: {time_change:.2%})",
                "details": {
                    "current_time": current_time,
                    "average_time": avg_time,
                    "change_percentage": time_change
                }
            }
    
    def _save_health_check(self, asset_key: str, check_result: Dict[str, Any]):
        """Save a health check result to the database"""
        with self.get_connection() as conn:
            conn.execute('''
                INSERT INTO health_checks 
                (asset_key, check_type, status, message, details)
                VALUES (?, ?, ?, ?, ?)
            ''', (
                asset_key,
                check_result['type'],
                check_result['status'],
                check_result['message'],
                json.dumps(check_result.get('details', {}))
            ))
            conn.commit()
    
    def _calculate_overall_health(self, checks_results: List[Dict[str, Any]]) -> HealthStatus:
        """Calculate overall health status based on individual checks"""
        if not checks_results:
            return HealthStatus.UNKNOWN
        
        # Count unhealthy checks
        unhealthy_count = sum(1 for check in checks_results if check['status'] == HealthStatus.UNHEALTHY.value)
        
        if unhealthy_count > 0:
            return HealthStatus.UNHEALTHY
        elif all(check['status'] == HealthStatus.HEALTHY.value for check in checks_results):
            return HealthStatus.HEALTHY
        else:
            return HealthStatus.UNKNOWN
    
    def _update_health_summary(self, asset_key: str, overall_status: HealthStatus, checks_results: List[Dict[str, Any]]):
        """Update the health summary for an asset"""
        with self.get_connection() as conn:
            # Get current summary
            row = conn.execute(
                'SELECT * FROM health_summary WHERE asset_key = ?',
                (asset_key,)
            ).fetchone()
            
            if row:
                # Update existing record
                failure_count = row['failure_count']
                last_healthy = row['last_healthy']
                
                if overall_status == HealthStatus.HEALTHY:
                    last_healthy = datetime.utcnow().isoformat()
                    failure_count = 0
                elif overall_status == HealthStatus.UNHEALTHY:
                    failure_count += 1
                
                conn.execute('''
                    UPDATE health_summary 
                    SET overall_status = ?, last_healthy = ?, failure_count = ?, last_checked = ?
                    WHERE asset_key = ?
                ''', (
                    overall_status.value,
                    last_healthy,
                    failure_count,
                    datetime.utcnow().isoformat(),
                    asset_key
                ))
            else:
                # Insert new record
                last_healthy = datetime.utcnow().isoformat() if overall_status == HealthStatus.HEALTHY else None
                failure_count = 1 if overall_status == HealthStatus.UNHEALTHY else 0
                
                conn.execute('''
                    INSERT INTO health_summary 
                    (asset_key, overall_status, last_healthy, failure_count, last_checked)
                    VALUES (?, ?, ?, ?, ?)
                ''', (
                    asset_key,
                    overall_status.value,
                    last_healthy,
                    failure_count,
                    datetime.utcnow().isoformat()
                ))
            
            conn.commit()
    
    def get_asset_health(self, asset_key: str) -> Optional[Dict[str, Any]]:
        """Get the current health status for an asset"""
        with self.get_connection() as conn:
            row = conn.execute(
                'SELECT * FROM health_summary WHERE asset_key = ?',
                (asset_key,)
            ).fetchone()
            
            if row:
                return {
                    "asset_key": row["asset_key"],
                    "overall_status": row["overall_status"],
                    "last_healthy": row["last_healthy"],
                    "failure_count": row["failure_count"],
                    "last_checked": row["last_checked"]
                }
            return None
    
    def get_unhealthy_assets(self) -> List[Dict[str, Any]]:
        """Get all unhealthy assets"""
        with self.get_connection() as conn:
            rows = conn.execute(
                'SELECT * FROM health_summary WHERE overall_status = ?',
                (HealthStatus.UNHEALTHY.value,)
            ).fetchall()
            
            return [{
                "asset_key": row["asset_key"],
                "overall_status": row["overall_status"],
                "last_healthy": row["last_healthy"],
                "failure_count": row["failure_count"],
                "last_checked": row["last_checked"]
            } for row in rows]

# Global instance
health_monitor = HealthMonitor()