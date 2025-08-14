import sqlite3
import json
from datetime import datetime
from typing import Dict, Any, Optional
from contextlib import contextmanager
from .metadata_store import metadata_store

class MonitoringSystem:
    """Comprehensive monitoring system for asset pipelines"""
    
    def __init__(self, db_path: str = "monitoring.db"):
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
        """Initialize the monitoring database"""
        with self.get_connection() as conn:
            # Metrics table
            conn.execute('''
                CREATE TABLE IF NOT EXISTS metrics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    pipeline_name TEXT,
                    asset_key TEXT,
                    metric_name TEXT,
                    metric_value REAL,
                    metadata TEXT, -- JSON serialized dict
                    recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Alerts table
            conn.execute('''
                CREATE TABLE IF NOT EXISTS alerts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    asset_key TEXT,
                    alert_type TEXT,
                    message TEXT,
                    severity TEXT,
                    metadata TEXT, -- JSON serialized dict
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    resolved_at TIMESTAMP NULL
                )
            ''')
            
            # SLA tracking table
            conn.execute('''
                CREATE TABLE IF NOT EXISTS sla_tracking (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    asset_key TEXT,
                    sla_name TEXT,
                    expected_value REAL,
                    actual_value REAL,
                    status TEXT, -- MET, MISSED, WARNING
                    recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            conn.commit()
    
    def record_metric(self, 
                     pipeline_name: str,
                     asset_key: str,
                     metric_name: str,
                     metric_value: float,
                     metadata: Optional[Dict[str, Any]] = None):
        """Record a metric for an asset"""
        with self.get_connection() as conn:
            conn.execute('''
                INSERT INTO metrics 
                (pipeline_name, asset_key, metric_name, metric_value, metadata)
                VALUES (?, ?, ?, ?, ?)
            ''', (
                pipeline_name,
                asset_key,
                metric_name,
                metric_value,
                json.dumps(metadata) if metadata else None
            ))
            conn.commit()
    
    def check_sla(self, 
                  asset_key: str,
                  sla_name: str,
                  expected_value: float,
                  actual_value: float,
                  threshold: float = 0.1):
        """Check if an SLA is met and record it"""
        # Determine SLA status
        if actual_value >= expected_value:
            status = "MET"
        elif actual_value >= expected_value * (1 - threshold):
            status = "WARNING"
        else:
            status = "MISSED"
        
        with self.get_connection() as conn:
            conn.execute('''
                INSERT INTO sla_tracking 
                (asset_key, sla_name, expected_value, actual_value, status)
                VALUES (?, ?, ?, ?, ?)
            ''', (asset_key, sla_name, expected_value, actual_value, status))
            conn.commit()
        
        # Create alert if SLA is missed or warning
        if status in ["MISSED", "WARNING"]:
            self.create_alert(
                asset_key=asset_key,
                alert_type="SLA_VIOLATION",
                message=f"SLA '{sla_name}' {status.lower()}: expected {expected_value}, got {actual_value}",
                severity="HIGH" if status == "MISSED" else "MEDIUM"
            )
    
    def create_alert(self, 
                    asset_key: str,
                    alert_type: str,
                    message: str,
                    severity: str = "MEDIUM",
                    metadata: Optional[Dict[str, Any]] = None):
        """Create an alert for an asset issue"""
        with self.get_connection() as conn:
            conn.execute('''
                INSERT INTO alerts 
                (asset_key, alert_type, message, severity, metadata)
                VALUES (?, ?, ?, ?, ?)
            ''', (
                asset_key,
                alert_type,
                message,
                severity,
                json.dumps(metadata) if metadata else None
            ))
            conn.commit()
        
        # Also save to asset execution metadata
        try:
            # This would be called in the context of an execution
            # We're just demonstrating the connection here
            pass
        except:
            # If we can't connect to the metadata store, that's okay
            pass
    
    def resolve_alert(self, alert_id: int):
        """Mark an alert as resolved"""
        with self.get_connection() as conn:
            conn.execute('''
                UPDATE alerts 
                SET resolved_at = ? 
                WHERE id = ?
            ''', (datetime.utcnow(), alert_id))
            conn.commit()
    
    def get_active_alerts(self, asset_key: Optional[str] = None) -> list:
        """Get unresolved alerts"""
        with self.get_connection() as conn:
            if asset_key:
                rows = conn.execute('''
                    SELECT * FROM alerts 
                    WHERE resolved_at IS NULL AND asset_key = ?
                    ORDER BY created_at DESC
                ''', (asset_key,)).fetchall()
            else:
                rows = conn.execute('''
                    SELECT * FROM alerts 
                    WHERE resolved_at IS NULL
                    ORDER BY created_at DESC
                ''').fetchall()
            
            return [{
                "id": row["id"],
                "asset_key": row["asset_key"],
                "alert_type": row["alert_type"],
                "message": row["message"],
                "severity": row["severity"],
                "metadata": json.loads(row["metadata"]) if row["metadata"] else None,
                "created_at": row["created_at"]
            } for row in rows]
    
    def get_recent_metrics(self, asset_key: str, metric_name: str, hours: int = 24) -> list:
        """Get recent metrics for an asset"""
        with self.get_connection() as conn:
            rows = conn.execute('''
                SELECT * FROM metrics 
                WHERE asset_key = ? AND metric_name = ? AND 
                recorded_at >= datetime('now', '-{} hours')
                ORDER BY recorded_at DESC
            '''.format(hours), (asset_key, metric_name)).fetchall()
            
            return [{
                "id": row["id"],
                "pipeline_name": row["pipeline_name"],
                "metric_name": row["metric_name"],
                "metric_value": row["metric_value"],
                "metadata": json.loads(row["metadata"]) if row["metadata"] else None,
                "recorded_at": row["recorded_at"]
            } for row in rows]

# Global instance
monitor = MonitoringSystem()