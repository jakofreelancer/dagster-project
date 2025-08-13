from dagster import AssetExecutionContext, asset, AssetIn
from my_dagster_project.shared.load_to_sql_utils import load_to_stg_table, sanitize_dataframe
from pandas import DataFrame

# Import enterprise features conditionally
try:
  from my_dagster_project.core.metadata_manager import metadata_manager
  from my_dagster_project.core.monitoring import monitor
  from my_dagster_project.core.logging_config import get_logger
  logger = get_logger(__name__)
  ENTERPRISE_ENABLED = True
except ImportError:
  ENTERPRISE_ENABLED = False
  import logging
  logger = logging.getLogger(__name__)

# Register loader asset - call this separately after initialization
def register_asset_metadata():
  """Register this asset in the metadata system - call this separately"""
  if ENTERPRISE_ENABLED:
    try:
      metadata_manager.register_asset(
        asset_key="blast.stg_blast_shotcrete_71",
        asset_name="stg_blast_shotcrete_71_data",
        asset_type="transform",
        group_name="stg__loaders",
        pipeline_name="blast_shotcrete_pipeline",
        owners=["javkhlanbu@ot.mn", "myagmarsurens@ot.mn"],
        tags={"domain": "blast", "stage": "staging"},
        dependencies=["blast.raw_blast_shotcrete_71"],
        metadata={
          "target_table": "sql71_blast_dagster_test",
          "target_schema": "stg"
        }
      )
    except Exception as e:
      logger.warning(f"Could not register asset metadata: {e}")

@asset(
  name="stg_blast_shotcrete_71_data",
  ins={"raw_blast_shotcrete_71_data": AssetIn()},
  group_name="stg__loaders",
  description="Raw blast shotcrete data for SQL staging, filtered by latest process per BlastID+Source.",
  compute_kind="pandas",
  owners=["javkhlanbu@ot.mn", "myagmarsurens@ot.mn"],
  tags={"domain": "blast", "source": "MineSys", "type": "raw"},  # MOVED type to tags
  # REMOVED: type="raw",  <-- This was causing the error
  metadata={
    "source_system": "MineSys SQL71",
    "target_schema": "stg",
    "load_window": "dynamic via get_load_window()",
    "owner": "javkhlanbu",
    "team": "Geology",
    "type": "raw",
    "schedule": "daily"
  },
  code_version="v1.3"
)
def stg_blast_shotcrete_71_data(
  context: AssetExecutionContext,
  raw_blast_shotcrete_71_data: DataFrame
):
  '''
  Loads the raw blast shotcrete data into a preparation table for further processing.
  Enhanced with metadata tracking and monitoring when available.
  '''
  
  if ENTERPRISE_ENABLED:
    logger.info("Starting blast shotcrete data loading to staging")
  
  try:
    sanitized_data = sanitize_dataframe(raw_blast_shotcrete_71_data)
    
    load_to_stg_table(
        context=context,
        df=sanitized_data,
        target_table_name='sql71_blast_dagster_test'
    )
    
    # Enterprise features if available
    if ENTERPRISE_ENABLED:
      try:
        # Record metrics
        monitor.record_metric(
          pipeline_name="blast_shotcrete_pipeline",
          metric_name="records_loaded",
          metric_value=len(sanitized_data),
          asset_key="blast.stg_blast_shotcrete_71"
        )
        
        # Save execution metadata
        metadata_manager.save_asset_execution(
          asset_key="blast.stg_blast_shotcrete_71",
          run_id=context.run_id,
          records_processed=len(sanitized_data),
          metadata={
            "target_table": "sql71_blast_dagster_test",
            "row_count": len(sanitized_data)
          }
        )
        
        logger.info(f"Successfully loaded {len(sanitized_data)} records to staging")
      except Exception as e:
        logger.warning(f"Could not save metrics/metadata: {e}")
      
  except Exception as e:
    if ENTERPRISE_ENABLED:
      logger.error(f"Loading failed: {str(e)}", exc_info=True)
      
      try:
        metadata_manager.save_asset_execution(
          asset_key="blast.stg_blast_shotcrete_71",
          run_id=context.run_id,
          status="failed",
          metadata={"error": str(e)}
        )
      except:
        pass  # Don't fail if metadata save fails
    
    raise

all_assets = [stg_blast_shotcrete_71_data]
