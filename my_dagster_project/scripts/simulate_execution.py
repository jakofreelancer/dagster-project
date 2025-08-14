"""
Script to simulate execution data for test asset
"""

import sys
import os

# Add the project root to the Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from my_dagster_project.core.metadata_store import metadata_store

def simulate_execution_data():
    """Simulate execution data for the test asset"""
    try:
        # Simulate some successful executions
        for i in range(3):
            metadata_store.save_asset_execution(
                asset_key="test.test_asset",
                run_id=f"test_run_{i}",
                status="success",
                records_processed=100 + i * 50,
                metadata={
                    "test": "data",
                    "iteration": i
                }
            )
        
        # Simulate a failed execution
        metadata_store.save_asset_execution(
            asset_key="test.test_asset",
            run_id="test_run_failed",
            status="failed",
            metadata={
                "error": "Test error for demonstration",
                "test": True
            }
        )
        
        print("Simulated execution data created successfully!")
        
        # Verify the execution data
        executions = metadata_store.get_asset_executions("test.test_asset", limit=10)
        print(f"Found {len(executions)} execution records:")
        for exec_data in executions:
            print(f"  - Run ID: {exec_data['run_id']}, Status: {exec_data['status']}")
            
    except Exception as e:
        print(f"Failed to simulate execution data: {e}")

if __name__ == "__main__":
    simulate_execution_data()