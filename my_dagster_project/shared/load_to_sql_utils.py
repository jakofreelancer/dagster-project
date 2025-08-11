import pandas as pd
import time
from dagster import AssetExecutionContext
from my_dagster_project.shared.config import get_config_value
from my_dagster_project.shared.db_utils import get_sql_connection, PANDAS_TO_SQLSERVER


def sanitize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
  """
    Fill null values in a DataFrame with appropriate defaults based on data type.
  """
  for col in df.columns:
    dtype = df[col].dtype
    if dtype == 'float64':
      df[col] = df[col].fillna(0.0)
    elif dtype == 'datetime64[ns]':
      df[col] = df[col].fillna(pd.NaT)
    elif dtype == 'object' or dtype.name == 'string':
      df[col] = df[col].fillna('')
    else:
      df[col] = df[col].where(pd.notnull(df[col]), None)
  return df


def get_sqlserver_columns_from_df(df: pd.DataFrame) -> str:
  """
    Generate a SQL Server column definition string from a DataFrame.
  """
  columns = []
  for col, dtype in df.dtypes.items():
    sql_type = PANDAS_TO_SQLSERVER.get(str(dtype), 'NVARCHAR(MAX)')
    columns.append(f'[{col}] {sql_type}')
  
  columns.extend([
    '[inserted_datetime] DATETIME2 DEFAULT GETDATE()',
    '[inserted_by_user] NVARCHAR(255) DEFAULT SYSTEM_USER',
    '[inserted_from_machine] NVARCHAR(255) DEFAULT HOST_NAME()',
  ])
  
  return ',\n'.join(columns)


def create_table_from_df(cursor, df: pd.DataFrame, schema: str, table: str):
  """
    Create a SQL Server table based on DataFrame schema.
  """
  if not schema.isidentifier() or not table.isidentifier():
    raise ValueError('Invalid schema or table name')

  columns_sql = get_sqlserver_columns_from_df(df)
  create_sql = f'''
    CREATE TABLE {schema}.{table} (
      {columns_sql}
    )
  '''
  cursor.execute(create_sql)


def create_table_from_df_if_not_exists(cursor, df: pd.DataFrame, schema: str, table: str) -> bool:
  """
    Check if a table exists. If not, create it from DataFrame schema.
  """
  check_sql = '''
    SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
  '''
  cursor.execute(check_sql, (schema, table))
  exists = cursor.fetchone()[0] > 0
  if exists:
    return False
  create_table_from_df(cursor, df, schema, table)
  return True


def load_to_stg_table(context: AssetExecutionContext, df: pd.DataFrame, target_table_name: str):
  """
    Loads a sanitized DataFrame to a staging SQL Server table using bulk insert.
    Includes automatic table creation if needed.
  """
  start_time = time.time()
  context.log.info("Fetching configuration values...")

  schema = get_config_value('staging_target_schema')
  server = get_config_value('sql_server')
  database = get_config_value('sql_database')

  stg_table = f'{schema}.{target_table_name}'

  # Validation
  context.log.info(f"Validating DataFrame for table: {target_table_name}")
  if not isinstance(df, pd.DataFrame):
    raise ValueError('Input is not a Pandas DataFrame.')
  if df.empty:
    raise ValueError('DataFrame is empty.')
  if not all(isinstance(col, str) for col in df.columns):
    raise ValueError('All column names must be strings.')

  context.log.info(f"DataFrame shape: {df.shape}, dtypes: {df.dtypes.to_dict()}")

  # Sanitize
  context.log.info("Sanitizing DataFrame...")
  df = sanitize_dataframe(df)
  sanitized_rows = df.values.tolist()

  insert_columns = list(df.columns)
  placeholders = ', '.join(['?'] * len(insert_columns))
  insert_sql = f"INSERT INTO {stg_table} ({', '.join(insert_columns)}) VALUES ({placeholders})"
  truncate_sql = f'TRUNCATE TABLE {stg_table}'

  # DB Insert
  with get_sql_connection(server, database) as conn:
    with conn.cursor() as cursor:
      try:
        context.log.info("Checking if table exists...")
        cursor.execute('''
          SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES
          WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
        ''', (schema, target_table_name))
        table_exists = cursor.fetchone()[0] > 0

        if not table_exists:
          context.log.info("Table does not exist. Creating...")
          create_table_from_df(cursor, df, schema, target_table_name)

        context.log.info("Ensuring table structure...")
        _ = create_table_from_df_if_not_exists(cursor, df, schema, target_table_name)

        context.log.info("Beginning transaction...")
        cursor.execute('BEGIN TRANSACTION')
        cursor.fast_executemany = True

        context.log.info("Truncating table...")
        cursor.execute(truncate_sql)

        context.log.info("Bulk inserting rows...")
        insert_start = time.time()
        cursor.executemany(insert_sql, sanitized_rows)
        context.log.info(f"Insert completed in {time.time() - insert_start:.2f}s")

        cursor.execute('COMMIT TRANSACTION')
        context.log.info(f"Inserted {len(sanitized_rows)} rows into {stg_table}")

      except Exception as e:
        cursor.execute('ROLLBACK TRANSACTION')
        context.log.error(f"Error during insert: {str(e)}")
        raise

  context.log.info(f"Total load time: {time.time() - start_time:.2f} seconds.")
