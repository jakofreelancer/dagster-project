import pandas as pd
from typing import Optional, List
from my_dagster_project.shared.db_utils import get_sql_connection

def ingest_from_sql(
  sql_server: str,
  sql_database: str,
  query: str,
  date_params: Optional[List] = None
) -> pd.DataFrame:
  """
    Executes a SQL query against a SQL Server and returns the results as a DataFrame.
    
    Args:
      sql_server (str): SQL Server host or instance name
      sql_database (str): Target SQL database
      query (str): SQL query to execute (parameterized or raw)
      date_params (list, optional): Optional list of parameters for the query
    
    Returns:
      pd.DataFrame: Query results as a DataFrame
  """
  with get_sql_connection(sql_server=sql_server, sql_database=sql_database) as conn:
    return pd.read_sql(query, conn, params=date_params if date_params else None)
