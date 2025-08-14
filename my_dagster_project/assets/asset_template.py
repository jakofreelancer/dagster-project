"""
Asset Template - Use this as a starting point for new assets
"""

import pandas as pd
from dagster import asset, AssetExecutionContext
from typing import Any

# TODO: Update asset name and group
@asset(
    name="template_asset_name",
    group_name="template_group",
    description="Template for creating new assets",
    compute_kind="pandas",
    owners=["your.email@company.com"],
    tags={
        "domain": "template_domain",
        "type": "template_type"
    },
    metadata={
        "purpose": "Template for new assets",
        "version": "1.0"
    }
)
def template_asset_name(context: AssetExecutionContext) -> Any:
    """
    Template function for creating new assets.
    
    TODO: 
    1. Update function name to match asset name
    2. Implement asset logic
    3. Update metadata and tags
    4. Add proper return type hint
    5. Add context logging as needed
    """
    
    # TODO: Implement your asset logic here
    # Example:
    # data = fetch_data_from_source()
    # processed_data = transform_data(data)
    # return processed_data
    
    # For now, return empty DataFrame as example
    df = pd.DataFrame()
    
    # Add execution metadata
    context.add_output_metadata({
        "row_count": len(df),
        "preview": "No data" if df.empty else df.head().to_markdown()
    })
    
    return df

# TODO: Add asset to all_assets list
# all_assets = [template_asset_name]