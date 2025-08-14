import os
import importlib.util
import inspect
from typing import List, Dict, Any, Set
from pathlib import Path
from dagster import AssetIn, AssetsDefinition
import yaml
from datetime import datetime
from my_dagster_project.core.asset_record import asset_record
from my_dagster_project.shared.system_info import get_system_info

class AssetDiscovery:
    """Automatically discover and register assets in the project"""
    
    def __init__(self, project_root: str = ".", config_path: str = "config/app_config.yaml"):
        self.project_root = Path(project_root).resolve()
        self.assets_root = self.project_root / "my_dagster_project" / "assets"
        self.config = self._load_config(config_path)
        self.system_info = get_system_info()
        self.last_scan_time = None
        self.discovered_assets = []
        
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load configuration for discovery settings"""
        full_path = self.project_root / config_path
        if full_path.exists():
            with open(full_path, 'r') as f:
                return yaml.safe_load(f) or {}
        return {}
    
    def get_discovery_interval(self) -> int:
        """Get discovery interval from config or default to 1 hour"""
        return self.config.get('asset_management', {}).get('auto_discovery_interval', 3600)
    
    def should_run_discovery(self) -> bool:
        """Check if discovery should run based on interval"""
        if self.last_scan_time is None:
            return True
            
        interval = self.get_discovery_interval()
        time_since_last = (datetime.utcnow() - self.last_scan_time).total_seconds()
        return time_since_last >= interval
    
    def discover_assets(self, force: bool = False) -> List[Dict[str, Any]]:
        """Discover all assets in the project"""
        # Check if we should run discovery
        if not force and not self.should_run_discovery():
            return self.discovered_assets
        
        assets = []
        
        # Walk through the assets directory
        if not self.assets_root.exists():
            self.last_scan_time = datetime.utcnow()
            return assets
            
        for root, dirs, files in os.walk(self.assets_root):
            # Skip __pycache__ directories
            dirs[:] = [d for d in dirs if d != '__pycache__']
            
            for file in files:
                if file.endswith(".py") and not file.startswith("__"):
                    file_path = Path(root) / file
                    try:
                        module_assets = self._extract_assets_from_module(file_path)
                        assets.extend(module_assets)
                    except Exception as e:
                        print(f"Warning: Could not parse {file_path}: {e}")
        
        self.discovered_assets = assets
        self.last_scan_time = datetime.utcnow()
        return assets
    
    def _extract_assets_from_module(self, module_path: Path) -> List[Dict[str, Any]]:
        """Extract asset definitions from a Python module"""
        assets = []
        
        # Get relative path for asset key generation
        try:
            rel_path = module_path.relative_to(self.assets_root)
            module_name = str(rel_path.with_suffix('')).replace(os.path.sep, ".")
            
            # Try to import the module
            spec = importlib.util.spec_from_file_location(module_name, module_path)
            if spec and spec.loader:
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
                
                # Look for all_assets variable first (common pattern in your code)
                if hasattr(module, 'all_assets'):
                    for asset in module.all_assets:
                        asset_info = self._extract_asset_info(asset, module_path, module_name)
                        if asset_info:
                            assets.append(asset_info)
                else:
                    # Look for individual asset functions
                    for name, obj in inspect.getmembers(module):
                        if hasattr(obj, '_dagster_asset') or hasattr(obj, 'asset_key'):
                            asset_info = self._extract_asset_info(obj, module_path, module_name, name)
                            if asset_info:
                                assets.append(asset_info)
        except Exception as e:
            print(f"Warning: Could not import module {module_path}: {e}")
        
        return assets
    
    def _extract_asset_info(self, asset_obj: Any, module_path: Path, module_name: str, asset_name: str = None) -> Dict[str, Any]:
        """Extract information from an asset object"""
        try:
            # Handle AssetsDefinition objects
            if isinstance(asset_obj, AssetsDefinition):
                # Get asset key
                asset_keys = asset_obj.keys
                if asset_keys:
                    asset_key_str = ".".join(asset_keys[0].path)
                else:
                    asset_key_str = f"{module_name}.{asset_name}" if asset_name else module_name
                
                # Get asset properties
                # For AssetsDefinition, we need to extract metadata differently
                description = getattr(asset_obj, 'description', '')
                group_name = getattr(asset_obj, 'group_names', ['default'])[0] if getattr(asset_obj, 'group_names', []) else 'default'
                
                # Get owners, tags, metadata from specs
                owners = []
                tags = {}
                metadata = {}
                
                # Get dependencies
                deps = []
                # For AssetsDefinition, dependencies are more complex to extract
                
                return {
                    "asset_key": asset_key_str,
                    "asset_name": asset_name or asset_key_str.split('.')[-1],
                    "module_path": str(module_path),
                    "module_name": module_name,
                    "description": description,
                    "group_name": group_name,
                    "owners": owners,
                    "tags": tags,
                    "metadata": metadata,
                    "dependencies": deps,
                    "system_info": self.system_info,
                    "discovered_at": datetime.utcnow().isoformat()
                }
            else:
                # Handle function-based assets (original code)
                # Get asset metadata
                asset_key = getattr(asset_obj, 'key', None)
                if asset_key:
                    asset_key_str = ".".join(asset_key.path)
                else:
                    asset_key_str = f"{module_name}.{asset_name}" if asset_name else module_name
                
                # Get asset properties
                description = getattr(asset_obj, 'description', '')
                group_name = getattr(asset_obj, 'group_name', 'default')
                owners = getattr(asset_obj, 'owners', [])
                tags = getattr(asset_obj, 'tags', {})
                metadata = getattr(asset_obj, 'metadata', {})
                
                # Get dependencies
                deps = []
                if hasattr(asset_obj, 'ins'):
                    for dep_name, dep_info in asset_obj.ins.items():
                        if isinstance(dep_info, AssetIn) and dep_info.asset_key:
                            deps.append(".".join(dep_info.asset_key.path))
                
                return {
                    "asset_key": asset_key_str,
                    "asset_name": asset_name or getattr(asset_obj, '__name__', asset_key_str.split('.')[-1]),
                    "module_path": str(module_path),
                    "module_name": module_name,
                    "description": description,
                    "group_name": group_name,
                    "owners": owners,
                    "tags": tags,
                    "metadata": metadata,
                    "dependencies": deps,
                    "system_info": self.system_info,
                    "discovered_at": datetime.utcnow().isoformat()
                }
        except Exception as e:
            print(f"Warning: Could not extract asset info: {e}")
            return None
    
    def register_discovered_assets(self) -> int:
        """Register all discovered assets with the asset record system"""
        registered_count = 0
        
        for asset_info in self.discovered_assets:
            try:
                # Check if asset should be updated
                if asset_record.register_or_update_asset(
                    asset_key=asset_info["asset_key"],
                    asset_name=asset_info["asset_name"],
                    asset_type=asset_info.get("tags", {}).get("type", "unknown"),
                    group_name=asset_info["group_name"],
                    pipeline_name=asset_info.get("tags", {}).get("pipeline", "default"),
                    owners=asset_info["owners"],
                    tags=asset_info["tags"],
                    metadata=asset_info["metadata"],
                    dependencies=asset_info.get("dependencies", []),
                    config=asset_info.get("config", {})
                ):
                    registered_count += 1
            except Exception as e:
                print(f"Failed to register asset {asset_info['asset_key']}: {e}")
        
        return registered_count
    
    def generate_asset_lineage(self) -> Dict[str, List[str]]:
        """Generate asset lineage based on asset dependencies"""
        lineage = {}
        
        for asset in self.discovered_assets:
            asset_key = asset['asset_key']
            dependencies = asset.get('dependencies', [])
            lineage[asset_key] = dependencies
        
        return lineage
    
    def get_modified_assets(self, since: datetime = None) -> List[Dict[str, Any]]:
        """Get assets that have been modified since a specific time"""
        if since is None:
            since = self.last_scan_time or datetime.utcnow()
            
        modified_assets = []
        
        for asset in self.discovered_assets:
            module_path = Path(asset['module_path'])
            if module_path.exists():
                mod_time = datetime.fromtimestamp(module_path.stat().st_mtime)
                if mod_time > since:
                    modified_assets.append(asset)
        
        return modified_assets

# Global instance
asset_discovery = AssetDiscovery()