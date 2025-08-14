import json
import sqlite3
from datetime import datetime
from typing import Dict, List, Optional, Any
from contextlib import contextmanager
from pathlib import Path
from my_dagster_project.shared.system_info import get_system_info

class MetadataStore:
    """Centralized metadata store for all Dagster assets"""
    
    def __init__(self, db_path: str = "metadata.db"):
        self.db_path = db_path
        self.system_info = get_system_info()
        self.init_db()
        self.upgrade_schema()
    
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
        """Initialize the metadata database with required tables"""
        with self.get_connection() as conn:
            # Asset definitions table
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
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Asset execution history table
            conn.execute('''
                CREATE TABLE IF NOT EXISTS asset_executions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    asset_key TEXT,
                    run_id TEXT,
                    status TEXT,
                    window_start TIMESTAMP,
                    window_end TIMESTAMP,
                    records_processed INTEGER,
                    metadata TEXT, -- JSON serialized dict
                    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    completed_at TIMESTAMP,
                    FOREIGN KEY (asset_key) REFERENCES assets (asset_key)
                )
            ''')
            
            # Asset lineage table
            conn.execute('''
                CREATE TABLE IF NOT EXISTS asset_lineage (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    upstream_asset_key TEXT,
                    downstream_asset_key TEXT,
                    relationship_type TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (upstream_asset_key) REFERENCES assets (asset_key),
                    FOREIGN KEY (downstream_asset_key) REFERENCES assets (asset_key)
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
            try:
                # Add updated_at column to assets table if it doesn't exist
                conn.execute('ALTER TABLE assets ADD COLUMN updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP')
                conn.commit()
            except sqlite3.OperationalError as e:
                # Column already exists, which is fine
                if "duplicate column name" not in str(e).lower():
                    raise
    
    def register_asset(self, 
                      asset_key: str,
                      asset_name: str,
                      asset_type: str,
                      group_name: str,
                      pipeline_name: str,
                      owners: List[str],
                      tags: Dict[str, str],
                      metadata: Dict[str, Any],
                      dependencies: Optional[List[str]] = None):
        """Register or update an asset in the metadata store"""
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
            conn.execute('''
                INSERT OR REPLACE INTO assets 
                (asset_key, asset_name, asset_type, group_name, pipeline_name, 
                 owners, tags, metadata, dependencies, system_info, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                json.dumps(self.system_info),
                datetime.utcnow()
            ))
            conn.commit()
    
    def save_asset_execution(self,
                            asset_key: str,
                            run_id: str,
                            status: str = "success",
                            window_start: Optional[datetime] = None,
                            window_end: Optional[datetime] = None,
                            records_processed: Optional[int] = None,
                            metadata: Optional[Dict[str, Any]] = None):
        """Save asset execution details"""
        with self.get_connection() as conn:
            conn.execute('''
                INSERT INTO asset_executions 
                (asset_key, run_id, status, window_start, window_end, 
                 records_processed, metadata, completed_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                asset_key,
                run_id,
                status,
                window_start,
                window_end,
                records_processed,
                json.dumps(metadata) if metadata else None,
                datetime.utcnow()
            ))
            conn.commit()
    
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
                    "system_info": json.loads(row["system_info"]) if row["system_info"] else None,
                    "created_at": row["created_at"],
                    "updated_at": row["updated_at"]
                }
            return None
    
    def get_asset_executions(self, asset_key: str, limit: int = 10) -> List[Dict]:
        """Get execution history for an asset"""
        with self.get_connection() as conn:
            rows = conn.execute('''
                SELECT * FROM asset_executions 
                WHERE asset_key = ? 
                ORDER BY started_at DESC 
                LIMIT ?
            ''', (asset_key, limit)).fetchall()
            
            return [{
                "id": row["id"],
                "run_id": row["run_id"],
                "status": row["status"],
                "window_start": row["window_start"],
                "window_end": row["window_end"],
                "records_processed": row["records_processed"],
                "metadata": json.loads(row["metadata"]) if row["metadata"] else None,
                "started_at": row["started_at"],
                "completed_at": row["completed_at"]
            } for row in rows]
    
    def get_all_assets(self) -> List[Dict]:
        """Retrieve all registered assets"""
        with self.get_connection() as conn:
            rows = conn.execute('SELECT * FROM assets').fetchall()
            
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
                "system_info": json.loads(row["system_info"]) if row["system_info"] else None,
                "created_at": row["created_at"],
                "updated_at": row["updated_at"]
            } for row in rows]
    
    def add_lineage_relationship(self, upstream_key: str, downstream_key: str, relationship_type: str = "dependency"):
        """Add a lineage relationship between assets"""
        with self.get_connection() as conn:
            # Check if relationship already exists
            existing = conn.execute('''
                SELECT id FROM asset_lineage 
                WHERE upstream_asset_key = ? AND downstream_asset_key = ?
            ''', (upstream_key, downstream_key)).fetchone()
            
            if not existing:
                conn.execute('''
                    INSERT INTO asset_lineage 
                    (upstream_asset_key, downstream_asset_key, relationship_type)
                    VALUES (?, ?, ?)
                ''', (upstream_key, downstream_key, relationship_type))
                conn.commit()
    
    def get_downstream_assets(self, asset_key: str) -> List[Dict]:
        """Get assets that depend on the specified asset"""
        with self.get_connection() as conn:
            rows = conn.execute('''
                SELECT a.* FROM assets a
                JOIN asset_lineage al ON a.asset_key = al.downstream_asset_key
                WHERE al.upstream_asset_key = ?
            ''', (asset_key,)).fetchall()
            
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
                "system_info": json.loads(row["system_info"]) if row["system_info"] else None
            } for row in rows]
    
    def get_upstream_assets(self, asset_key: str) -> List[Dict]:
        """Get assets that the specified asset depends on"""
        with self.get_connection() as conn:
            rows = conn.execute('''
                SELECT a.* FROM assets a
                JOIN asset_lineage al ON a.asset_key = al.upstream_asset_key
                WHERE al.downstream_asset_key = ?
            ''', (asset_key,)).fetchall()
            
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
                "system_info": json.loads(row["system_info"]) if row["system_info"] else None
            } for row in rows]
    
    def get_asset_lineage(self, asset_key: str) -> Dict[str, List[Dict]]:
        """Get both upstream and downstream lineage for an asset"""
        return {
            "upstream": self.get_upstream_assets(asset_key),
            "downstream": self.get_downstream_assets(asset_key)
        }

# Global instance
metadata_store = MetadataStore()