import pytest
from unittest.mock import patch, MagicMock
from my_dagster_project.shared.db_utils import get_sql_connection

@pytest.fixture
def mock_connection():
  # Mock the pyodbc connection
  mock_conn = MagicMock()
  return mock_conn

@patch("my_dagster_project.shared.db_utils.pyodbc.connect")
def test_get_sql_connection(mock_pyodbc_connect, mock_connection):
  # Mock the pyodbc.connect method to return the mock connection
  mock_pyodbc_connect.return_value = mock_connection

  connection = get_sql_connection("mock_server", "mock_database")
  assert connection is not None
  mock_pyodbc_connect.assert_called_once_with(
    "DRIVER={ODBC Driver 17 for SQL Server};SERVER=mock_server;DATABASE=mock_database;Trusted_Connection=yes;"
  )
