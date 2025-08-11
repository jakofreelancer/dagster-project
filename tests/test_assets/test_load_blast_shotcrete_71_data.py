#import pytest
from my_dagster_project.assets.loaders.stg_blast_shotcrete_71_loader import stg_blast_shotcrete_71_data

def test_stg_blast_shotcrete_71_data():
  # Mock input data
  mock_data = ...  # Add mock DataFrame or input data
  result = stg_blast_shotcrete_71_data(mock_data)
  assert result is not None
  assert len(result) > 0
