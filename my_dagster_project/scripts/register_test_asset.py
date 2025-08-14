"""
Script to manually register a test asset
"""

import sys
import os

# Add the project root to the Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from my_dagster_project.core.asset_record import asset_record
from my_dagster_project.core.metadata_store import metadata_store
from my_dagster_project.shared.system_info import get_system_info

def register_test_asset():
    """Manually register a test asset"""
    try:
        # Get system information
        system_info = get_system_info()
        print(f"System Information: {system_info}")
        
        # Register a test asset in the asset record system
        updated = asset_record.register_or_update_asset(
            asset_key="test.test_asset",
            asset_name="test_asset",
            asset_type="demo",
            group_name="test_group",
            pipeline_name="test_pipeline",
            owners=["test@example.com"],
            tags={"domain": "test", "type": "demo"},
            metadata={
                "purpose": "testing",
                "complexity": "simple"
            },
            dependencies=[],
            config={}
        )
        
        if updated:
            print("Test asset registered successfully in asset record system!")
        else:
            print("Test asset was not updated in asset record system (may have been checked recently)")
            
        # Also register in metadata store for backward compatibility
        metadata_store.register_asset(
            asset_key="test.test_asset",
            asset_name="test_asset",
            asset_type="demo",
            group_name="test_group",
            pipeline_name="test_pipeline",
            owners=["test@example.com"],
            tags={"domain": "test", "type": "demo"},
            metadata={
                "purpose": "testing",
                "complexity": "simple"
            },
            dependencies=[]
        )
        
        print("Test asset registered successfully in metadata store!")
            
        # Verify the asset was registered in both systems with system info
        asset = asset_record.get_asset("test.test_asset")
        if asset:
            print(f"Asset found in record system: {asset['asset_key']} - {asset['asset_name']}")
            if asset.get('system_info'):
                print(f"  System Info: {asset['system_info']}")
            if asset.get('metadata', {}).get('system_info'):
                print(f"  Metadata System Info: {asset['metadata']['system_info']}")
        else:
            print("Asset not found in record system")
            
        asset = metadata_store.get_asset("test.test_asset")
        if asset:
            print(f"Asset found in metadata store: {asset['asset_key']} - {asset['asset_name']}")
            if asset.get('system_info'):
                print(f"  System Info: {asset['system_info']}")
            if asset.get('metadata', {}).get('system_info'):
                print(f"  Metadata System Info: {asset['metadata']['system_info']}")
        else:
            print("Asset not found in metadata store")
            
    except Exception as e:
        print(f"Failed to register test asset: {e}")

if __name__ == "__main__":
    register_test_asset()