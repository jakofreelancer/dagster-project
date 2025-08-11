import pandas as pd
from dagster import asset
from my_dagster_project.shared.load_window import get_load_window
from my_dagster_project.shared.ingest_utils import ingest_from_sql

@asset(
    name="raw_blast_shotcrete_71_data",
    group_name="stg__ingestion",
    description="Raw blast shotcrete data for SQL staging, filtered by latest process per BlastID+Source.",
    compute_kind="pandas",
    owners=["javkhlanbu@ot.mn", "myagmarsurens@ot.mn"],
    tags={"domain": "blast", "source": "MineSys"},
    type="stg",
    metadata={
        "source_system": "MineSys SQL71",
        "target_schema": "stg",
        "load_window": "dynamic via get_load_window()",
        "owner": "javkhlanbu",
        "team": "Geology",
        "type": "raw",
        "schedule": "daily"
    },
    code_version="v1.2"
)
def raw_blast_shotcrete_71_data() -> pd.DataFrame:
    '''
        Ingests raw blast shotcrete data from SQL Server MNOYTSQL71, database OTUGBlastLogSheet.
        The data is filtered to include only the latest blast process for each BlastID and Source.
        The query retrieves relevant columns and ensures that the data is within the specified load window.
    '''

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

    df = ingest_from_sql(
        sql_server='MNOYTSQL71',
        sql_database='OTUGBlastLogSheet',
        query=query,
        date_params=[start, end]
    )
    print(df.dtypes)
    print(df.head())

    return df

all_assets = [raw_blast_shotcrete_71_data]
