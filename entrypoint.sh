#!/bin/bash
set -e

# Print environment info
echo "Starting Dagster application..."
echo "Environment: ${ENVIRONMENT:-development}"
echo "Project Name: ${PROJECT_NAME:-dagster-project}"
echo "Dagster Home: ${DAGSTER_HOME:-/opt/dagster/dagster_home}"

# Initialize database if needed
if [ "$INIT_DB" = "true" ]; then
    echo "Initializing database..."
    python scripts/init_db.py
fi

# Run database migrations if needed
# python manage.py migrate

# Start the application
exec "$@"