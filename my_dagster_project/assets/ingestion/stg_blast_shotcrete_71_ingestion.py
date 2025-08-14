import pandas as pd
from dagster import asset, Output, MetadataValue
from my_dagster_project.shared.load_window import get_load_window
from my_dagster_project.shared.ingest_utils import ingest_from_sql

# Import enterprise features
from my_dagster_project.core.metadata_store import metadata_store
from my_dagster_project.core.monitoring import monitor
import logging
logger = logging.getLogger(__name__)

@asset(
    name="raw_blast_shotcrete_71_data",
    group_name="stg__ingestion",
    description="Raw blast shotcrete data for SQL staging, filtered by latest process per BlastID+Source.",
    compute_kind="pandas",
    owners=["javkhlanbu@ot.mn", "myagmarsurens@ot.mn"],
    tags={"domain": "blast", "source": "SQL71", "type": "stg"},
    metadata={
        "source_system": "[MNOYTSQL71].[OTUGBlastLogSheet]",
        "target_schema": "stg",
        "load_window": "dynamic via get_load_window()",
        "owner": "javkhlanbu",
        "team": "Geology",
        "type": "raw",
        "schedule": "daily"
    },
    code_version="v1.3"
)
def raw_blast_shotcrete_71_data(context) -> pd.DataFrame:
    '''
    Ingests raw blast shotcrete data from SQL Server MNOYTSQL71, database OTUGBlastLogSheet.
    Enhanced with metadata tracking and monitoring.
    '''
    
    logger.info("Starting blast shotcrete data ingestion")

    query = '''
        WITH BlastRelatedProcess AS (
            SELECT
                s.StageName,
                ak.StageID,
                ss.SubStageName,
                ak.SubStageID,
                ak.LastUpdatedDatetime BlastDateTime,
                ROW_NUMBER() OVER(PARTITION BY a.BlastID, Source ORDER BY ak.StageID DESC, ak.SubStageID DESC) rn_by_process_status,
                a.*
            FROM dbo.AdvanceBlast a
                LEFT JOIN dbo.Acknowledgement ak ON ak.BlastID = a.BlastID
                LEFT JOIN dbo.Stage s ON s.StageID = ak.StageID
                LEFT JOIN dbo.SubStage ss ON ss.SubStageID = ak.SubStageID
            WHERE ak.LastUpdatedDatetime IS NOT NULL
        ),
        AllBlasts AS (
            SELECT
                ROW_NUMBER() OVER (PARTITION BY BlastID, Source ORDER BY DailyAdvance DESC) rn_by_startChainage,
                b.*
            FROM BlastRelatedProcess b
            WHERE rn_by_process_status = 1
        ),
        CleanBlasts AS (
            SELECT
                BlastDateTime,
                Source,
                IsChecked,
                StartChainage,
                EndChainage,
                DailyAdvance,
                ChainageDirection,
                CreatedBy,
                CreatedDate,
                ModifiedBy,
                ModifiedDate,
                AdvanceID,
                ExcavationType,
                StageName,
                SubStageName
            FROM AllBlasts
            WHERE rn_by_startChainage = 1 AND Source IS NOT NULL
        )
        SELECT *
        FROM CleanBlasts
        WHERE BlastDateTime >= ? AND BlastDateTime < ?
    '''

    start, end = get_load_window()
    logger.info(f"Load window: {start} to {end}")

    try:
        df = ingest_from_sql(
            sql_server='MNOYTSQL71',
            sql_database='OTUGBlastLogSheet',
            query=query,
            date_params=[start, end]
        )
        
        # Log metrics
        monitor.record_metric(
            pipeline_name="blast_shotcrete_pipeline",
            metric_name="records_ingested",
            metric_value=len(df),
            asset_key="blast.raw_blast_shotcrete_71"
        )
        
        # Save execution metadata
        metadata_store.save_asset_execution(
            asset_key="blast.raw_blast_shotcrete_71",
            run_id=context.run_id,
            window_start=start,
            window_end=end,
            records_processed=len(df),
            metadata={
                "columns": list(df.columns),
                "dtypes": {str(k): str(v) for k, v in df.dtypes.to_dict().items()}
            }
        )
        
        logger.info(f"Successfully ingested {len(df)} records")
        
        # Add Dagster UI metadata
        context.add_output_metadata({
            "row_count": len(df),
            "window_start": str(start),
            "window_end": str(end),
            "preview": MetadataValue.md(df.head().to_markdown()) if not df.empty else "No data"
        })
        
        print(df.dtypes)
        print(df.head())
        
        return df
        
    except Exception as e:
        logger.error(f"Ingestion failed: {str(e)}", exc_info=True)
        
        try:
            metadata_store.save_asset_execution(
                asset_key="blast.raw_blast_shotcrete_71",
                run_id=context.run_id,
                window_start=start,
                window_end=end,
                status="failed",
                metadata={"error": str(e)}
            )
        except:
            pass  # Don't fail if metadata save fails
        
        raise

# Register asset metadata
metadata_store.register_asset(
    asset_key="blast.raw_blast_shotcrete_71",
    asset_name="raw_blast_shotcrete_71_data",
    asset_type="source",
    group_name="stg__ingestion",
    pipeline_name="blast_shotcrete_pipeline",
    owners=["javkhlanbu@ot.mn", "myagmarsurens@ot.mn"],
    tags={"domain": "blast", "source": "MineSys"},
    metadata={
        "source_system": "MineSys SQL71",
        "target_schema": "stg",
        "schedule": "daily"
    }
)

all_assets = [raw_blast_shotcrete_71_data]
