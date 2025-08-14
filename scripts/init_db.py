#!/usr/bin/env python
"""
Database initialization script - SQLite version
"""

import os
import sys
import time
import sqlite3

def initialize_sqlite_databases():
    """Initialize SQLite databases"""
    dagster_home = os.environ.get('DAGSTER_HOME', '/opt/dagster/dagster_home')
    
    # Create directories if they don't exist
    os.makedirs(dagster_home, exist_ok=True)
    os.makedirs(os.path.join(dagster_home, 'compute_logs'), exist_ok=True)
    os.makedirs(os.path.join(dagster_home, 'storage'), exist_ok=True)
    
    # SQLite databases are automatically created when first accessed
    # We'll just ensure the directory exists and is writable
    try:
        # Test database connection
        test_db_path = os.path.join(dagster_home, 'test.db')
        conn = sqlite3.connect(test_db_path)
        conn.execute('CREATE TABLE IF NOT EXISTS test (id INTEGER)')
        conn.close()
        os.remove(test_db_path)  # Clean up test database
        print("SQLite database directory is ready!")
        return True
    except Exception as e:
        print(f"Error initializing SQLite databases: {e}")
        return False

def main():
    """Main function"""
    print("Initializing SQLite databases...")
    if not initialize_sqlite_databases():
        sys.exit(1)
    
    print("Database initialization completed successfully!")

if __name__ == "__main__":
    main()