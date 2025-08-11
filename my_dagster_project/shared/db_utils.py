import pyodbc
import logging

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

logger = logging.getLogger(__name__)

def get_sql_connection(sql_server: str, sql_database: str) -> pyodbc.Connection:
    """
        Establish a new connection to a SQL Server database using Windows Authentication.
        
        Args:
            sql_server (str): SQL Server name or IP
            sql_database (str): Target database name
        
        Returns:
            pyodbc.Connection: Active database connection
        
        Raises:
            Exception: If connection fails
    """
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
        logger.error(f"Connection failed: {e}")
        raise


def is_connection_alive(conn: pyodbc.Connection) -> bool:
    """
        Check if a SQL Server connection is still alive.

        Args:
            conn (pyodbc.Connection): Existing SQL Server connection

        Returns:
            bool: True if alive, False if broken
    """ 
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        return True
    except (pyodbc.ProgrammingError, pyodbc.OperationalError):
        return False
