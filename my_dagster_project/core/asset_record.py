import sqlite3
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Union
from contextlib import contextmanager
from pathlib import Path
from my_dagster_project.shared.system_info import get_system_info

class FlexibleAssetRecord:
    """Flexible centralized asset record system with configurable update intervals"""
    
    def __init__(self, db_path: str = "assets.db", config_path: str = "config/app_config.yaml"):
        self.db_path = db_path
        self.config_path = config_path
        self.config = self._load_config()
        self.system_info = get_system_info()
        self.init_db()
        self.upgrade_schema()
    
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from YAML file"""
        try:
            import yaml
            config_file = Path(self.config_path)
            if config_file.exists():
                with open(config_file, 'r') as f:
                    return yaml.safe_load(f) or {}
        except:
            pass
        return {}
    
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
        """Initialize the asset record database with flexible schema"""
        with self.get_connection() as conn:
            # Main assets table with JSON fields for flexibility
            conn.execute('''
                CREATE TABLE IF NOT EXISTS assets (
                    asset_key TEXT PRIMARY KEY,
                    asset_name TEXT NOT NULL,
                    asset_type TEXT,
                    group_name TEXT,
                    pipeline_name TEXT,
                    owners TEXT,  -- JSON serialized list
                    tags TEXT,    -- JSON serialized dict
                    metadata TEXT, -- JSON serialized dict
                    dependencies TEXT, -- JSON serialized list
                    config TEXT,  -- JSON serialized dict for asset-specific config
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_checked TIMESTAMP,
                    version INTEGER DEFAULT 1
                )
            ''')
            
            # Asset schema table for data structure tracking
            conn.execute('''
                CREATE TABLE IF NOT EXISTS asset_schemas (
                    asset_key TEXT,
                    column_name TEXT,
                    data_type TEXT,
                    is_nullable BOOLEAN,
                    last_seen TIMESTAMP,
                    PRIMARY KEY (asset_key, column_name)
                )
            ''')
            
            # Asset metrics table
            conn.execute('''
                CREATE TABLE IF NOT EXISTS asset_metrics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    asset_key TEXT,
                    metric_name TEXT,
                    metric_value REAL,
                    recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (asset_key) REFERENCES assets (asset_key)
                )
            ''')
            
            # Asset alerts table
            conn.execute('''
                CREATE TABLE IF NOT EXISTS asset_alerts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    asset_key TEXT,
                    alert_type TEXT,
                    severity TEXT,
                    message TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    resolved_at TIMESTAMP NULL,
                    FOREIGN KEY (asset_key) REFERENCES assets (asset_key)
                )
            ''')
            
            conn.commit()
    
    def upgrade_schema(self):
        """Upgrade database schema to add new columns"""
        with self.get_connection() as conn:
            try:
                # Add system_info column to assets table if it doesn't exist
                conn.execute('ALTER TABLE assets ADD COLUMN system_info TEXT')
                conn.commit()
            except sqlite3.OperationalError as e:
                # Column already exists, which is fine
                if "duplicate column name" not in str(e).lower():
                    raise
    
    def get_update_interval(self) -> int:
        """Get asset update interval from config (default 15 minutes)"""
        return self.config.get('asset_management', {}).get('update_interval', 900)  # 15 minutes in seconds
    
    def should_update_asset(self, asset_key: str) -> bool:
        """Check if an asset should be updated based on interval"""
        interval = self.get_update_interval()
        
        with self.get_connection() as conn:
            row = conn.execute(
                'SELECT last_checked FROM assets WHERE asset_key = ?',
                (asset_key,)
            ).fetchone()
            
            if not row or not row['last_checked']:
                return True
            
            last_checked = datetime.fromisoformat(row['last_checked'])
            time_since_check = (datetime.utcnow() - last_checked).total_seconds()
            
            return time_since_check >= interval
    
    def register_or_update_asset(self, 
                               asset_key: str,
                               asset_name: str,
                               asset_type: str,
                               group_name: str,
                               pipeline_name: str,
                               owners: List[str],
                               tags: Dict[str, str],
                               metadata: Dict[str, Any],
                               dependencies: Optional[List[str]] = None,
                               config: Optional[Dict[str, Any]] = None) -> bool:
        """Register or update an asset record. Returns True if updated, False if not."""
        # Check if we should update this asset
        if not self.should_update_asset(asset_key):
            return False
        
        # Enhance metadata with system information
        enhanced_metadata = metadata.copy() if metadata else {}
        enhanced_metadata.update({
            "system_info": self.system_info,
            "registration_timestamp": datetime.utcnow().isoformat()
        })
        
        # Enhance tags with environment information
        enhanced_tags = tags.copy() if tags else {}
        enhanced_tags.update({
            "environment": self.system_info["environment"],
            "project": self.system_info["project_name"]
        })
        
        with self.get_connection() as conn:
            # Check if asset exists
            existing = conn.execute(
                'SELECT version FROM assets WHERE asset_key = ?',
                (asset_key,)
            ).fetchone()
            
            if existing:
                # Update existing asset
                version = existing['version'] + 1
                conn.execute('''
                    UPDATE assets 
                    SET asset_name = ?, asset_type = ?, group_name = ?, pipeline_name = ?,
                        owners = ?, tags = ?, metadata = ?, dependencies = ?, config = ?,
                        system_info = ?, last_updated = ?, last_checked = ?, version = ?
                    WHERE asset_key = ?
                ''', (
                    asset_name,
                    asset_type,
                    group_name,
                    pipeline_name,
                    json.dumps(owners),
                    json.dumps(enhanced_tags),
                    json.dumps(enhanced_metadata),
                    json.dumps(dependencies) if dependencies else None,
                    json.dumps(config) if config else None,
                    json.dumps(self.system_info),
                    datetime.utcnow().isoformat(),
                    datetime.utcnow().isoformat(),
                    version,
                    asset_key
                ))
            else:
                # Insert new asset
                conn.execute('''
                    INSERT INTO assets 
                    (asset_key, asset_name, asset_type, group_name, pipeline_name,
                     owners, tags, metadata, dependencies, config, system_info, last_checked)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    asset_key,
                    asset_name,
                    asset_type,
                    group_name,
                    pipeline_name,
                    json.dumps(owners),
                    json.dumps(enhanced_tags),
                    json.dumps(enhanced_metadata),
                    json.dumps(dependencies) if dependencies else None,
                    json.dumps(config) if config else None,
                    json.dumps(self.system_info),
                    datetime.utcnow().isoformat()
                ))
            
            conn.commit()
            return True
    
    def get_asset(self, asset_key: str) -> Optional[Dict]:
        """Retrieve asset information by key"""
        with self.get_connection() as conn:
            row = conn.execute(
                'SELECT * FROM assets WHERE asset_key = ?', 
                (asset_key,)
            ).fetchone()
            
            if row:
                return {
                    "asset_key": row["asset_key"],
                    "asset_name": row["asset_name"],
                    "asset_type": row["asset_type"],
                    "group_name": row["group_name"],
                    "pipeline_name": row["pipeline_name"],
                    "owners": json.loads(row["owners"]),
                    "tags": json.loads(row["tags"]),
                    "metadata": json.loads(row["metadata"]),
                    "dependencies": json.loads(row["dependencies"]) if row["dependencies"] else None,
                    "config": json.loads(row["config"]) if row["config"] else None,
                    "system_info": json.loads(row["system_info"]) if row["system_info"] else None,
                    "last_updated": row["last_updated"],
                    "last_checked": row["last_checked"],
                    "version": row["version"]
                }
            return None
    
    def get_all_assets(self, include_inactive: bool = False) -> List[Dict]:
        """Retrieve all registered assets"""
        with self.get_connection() as conn:
            if include_inactive:
                rows = conn.execute('SELECT * FROM assets').fetchall()
            else:
                # Only get assets that have been checked recently
                interval = self.get_update_interval()
                cutoff_time = datetime.utcnow() - timedelta(seconds=interval * 8)  # 8 intervals old
                rows = conn.execute(
                    'SELECT * FROM assets WHERE last_checked >= ?',
                    (cutoff_time.isoformat(),)
                ).fetchall()
            
            return [{
                "asset_key": row["asset_key"],
                "asset_name": row["asset_name"],
                "asset_type": row["asset_type"],
                "group_name": row["group_name"],
                "pipeline_name": row["pipeline_name"],
                "owners": json.loads(row["owners"]),
                "tags": json.loads(row["tags"]),
                "metadata": json.loads(row["metadata"]),
                "dependencies": json.loads(row["dependencies"]) if row["dependencies"] else None,
                "config": json.loads(row["config"]) if row["config"] else None,
                "system_info": json.loads(row["system_info"]) if row["system_info"] else None,
                "last_updated": row["last_updated"],
                "last_checked": row["last_checked"],
                "version": row["version"]
            } for row in rows]
    
    def update_asset_schema(self, asset_key: str, schema_info: List[Dict[str, Any]]):
        """Update the schema information for an asset"""
        with self.get_connection() as conn:
            # Clear existing schema for this asset
            conn.execute('DELETE FROM asset_schemas WHERE asset_key = ?', (asset_key,))
            
            # Insert new schema information
            for col_info in schema_info:
                conn.execute('''
                    INSERT INTO asset_schemas 
                    (asset_key, column_name, data_type, is_nullable, last_seen)
                    VALUES (?, ?, ?, ?, ?)
                ''', (
                    asset_key,
                    col_info['column_name'],
                    col_info['data_type'],
                    col_info.get('is_nullable', True),
                    datetime.utcnow().isoformat()
                ))
            
            conn.commit()
    
    def record_asset_metric(self, asset_key: str, metric_name: str, metric_value: Union[int, float]):
        """Record a metric for an asset"""
        with self.get_connection() as conn:
            conn.execute('''
                INSERT INTO asset_metrics 
                (asset_key, metric_name, metric_value)
                VALUES (?, ?, ?)
            ''', (asset_key, metric_name, float(metric_value)))
            conn.commit()
    
    def create_asset_alert(self, asset_key: str, alert_type: str, severity: str, message: str):
        """Create an alert for an asset"""
        with self.get_connection() as conn:
            conn.execute('''
                INSERT INTO asset_alerts 
                (asset_key, alert_type, severity, message)
                VALUES (?, ?, ?, ?)
            ''', (asset_key, alert_type, severity, message))
            conn.commit()
    
    def resolve_asset_alert(self, alert_id: int):
        """Mark an alert as resolved"""
        with self.get_connection() as conn:
            conn.execute('''
                UPDATE asset_alerts 
                SET resolved_at = ?
                WHERE id = ?
            ''', (datetime.utcnow().isoformat(), alert_id))
            conn.commit()
    
    def get_active_alerts(self, asset_key: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get unresolved alerts for assets"""
        with self.get_connection() as conn:
            if asset_key:
                rows = conn.execute('''
                    SELECT * FROM asset_alerts 
                    WHERE asset_key = ? AND resolved_at IS NULL
                    ORDER BY created_at DESC
                ''', (asset_key,)).fetchall()
            else:
                rows = conn.execute('''
                    SELECT * FROM asset_alerts 
                    WHERE resolved_at IS NULL
                    ORDER BY created_at DESC
                ''').fetchall()
            
            return [{
                "id": row["id"],
                "asset_key": row["asset_key"],
                "alert_type": row["alert_type"],
                "severity": row["severity"],
                "message": row["message"],
                "created_at": row["created_at"]
            } for row in rows]

# Global instance
asset_record = FlexibleAssetRecord()