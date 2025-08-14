from dagster import asset
import pandas as pd

@asset(
    name="test_asset",
    group_name="test_group",
    description="A simple test asset for demonstration purposes",
    compute_kind="pandas",
    owners=["test@example.com"],
    tags={"domain": "test", "type": "demo"},
    metadata={
        "purpose": "testing",
        "complexity": "simple"
    }
)
def test_asset() -> pd.DataFrame:
    """A simple test asset that creates a small DataFrame"""
    data = {
        'id': [1, 2, 3, 4, 5],
        'name': ['Alice', 'Bob', 'Charlie', 'Diana', 'Eve'],
        'value': [10, 20, 30, 40, 50]
    }
    return pd.DataFrame(data)

# Make sure to define all_assets as a list
all_assets = [test_asset]