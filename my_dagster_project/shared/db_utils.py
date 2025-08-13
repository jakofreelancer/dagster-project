import pyodbc
from my_dagster_project.core.logging_config import get_logger
from my_dagster_project.core.db_config import db_config as enterprise_db_config

# Mapping Pandas dtypes to SQL Server types
PANDAS_TO_SQLSERVER = {
    'object': 'NVARCHAR(MAX)',
    'int64': 'BIGINT',
    'float64': 'FLOAT',
    'bool': 'BIT',
    'datetime64[ns]': 'DATETIME2',
    'timedelta[ns]': 'TIME',
    'int32': 'INT',
    'float32': 'REAL'
}

logger = get_logger(__name__)

def get_sql_connection(sql_server: str, sql_database: str) -> pyodbc.Connection:
    """
    Enhanced connection function with environment support
    """
    # Check if this is a known source database
    if sql_server == 'MNOYTSQL71':
        return enterprise_db_config.get_connection('source', 'sql71')
    
    # Otherwise use standard connection
    connection_string = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={sql_server};"
        f"DATABASE={sql_database};"
        f"Trusted_Connection=yes;"
    )
    
    try:
        logger.info(f"Connecting to SQL Server: {sql_server}, DB: {sql_database}")
        conn = pyodbc.connect(connection_string)
        logger.info("SQL Server connection established successfully.")
        return conn
    except Exception as e:
        logger.error(f"Connection failed: {e}", exc_info=True)
        raise

def is_connection_alive(conn: pyodbc.Connection) -> bool:
    """Check if a SQL Server connection is still alive."""
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        return True
    except (pyodbc.ProgrammingError, pyodbc.OperationalError):
        return False
