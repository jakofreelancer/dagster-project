#!/usr/bin/env python
"""
Database initialization script for Docker deployment
"""

import os
import sys
import time
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

def wait_for_postgres():
    """Wait for PostgreSQL to be ready"""
    max_attempts = 30
    attempt = 0
    
    while attempt < max_attempts:
        try:
            conn = psycopg2.connect(
                host=os.environ.get('POSTGRES_HOST', 'postgres'),
                port=os.environ.get('POSTGRES_PORT', '5432'),
                user=os.environ.get('POSTGRES_USER', 'dagster'),
                password=os.environ.get('POSTGRES_PASSWORD', 'dagster_password'),
                database='postgres'  # Connect to default database first
            )
            conn.close()
            print("PostgreSQL is ready!")
            return True
        except psycopg2.OperationalError as e:
            attempt += 1
            print(f"Attempt {attempt}: PostgreSQL not ready yet. Waiting...")
            time.sleep(2)
    
    print("Failed to connect to PostgreSQL after maximum attempts")
    return False

def create_database():
    """Create the Dagster database if it doesn't exist"""
    try:
        # Connect to PostgreSQL server
        conn = psycopg2.connect(
            host=os.environ.get('POSTGRES_HOST', 'postgres'),
            port=os.environ.get('POSTGRES_PORT', '5432'),
            user=os.environ.get('POSTGRES_USER', 'dagster'),
            password=os.environ.get('POSTGRES_PASSWORD', 'dagster_password'),
            database='postgres'  # Connect to default database
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
        # Check if database exists
        cursor.execute("SELECT 1 FROM pg_catalog.pg_database WHERE datname = %s", ('dagster_metadata',))
        exists = cursor.fetchone()
        
        if not exists:
            print("Creating dagster_metadata database...")
            cursor.execute("CREATE DATABASE dagster_metadata")
            print("Database created successfully!")
        else:
            print("Database dagster_metadata already exists.")
        
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        print(f"Error creating database: {e}")
        return False

def main():
    """Main function"""
    print("Waiting for PostgreSQL to be ready...")
    if not wait_for_postgres():
        sys.exit(1)
    
    print("Initializing database...")
    if not create_database():
        sys.exit(1)
    
    print("Database initialization completed successfully!")

if __name__ == "__main__":
    main()