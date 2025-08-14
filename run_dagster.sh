#!/bin/bash
# Script to set environment variables for different deployment environments

# Set environment variables
export ENVIRONMENT=development
export PROJECT_NAME=dagster-project

# For production environment, you would use:
# export ENVIRONMENT=production
# export PROJECT_NAME=your-production-project-name

# For staging environment, you would use:
# export ENVIRONMENT=staging
# export PROJECT_NAME=your-staging-project-name

echo "Environment variables set:"
echo "ENVIRONMENT=$ENVIRONMENT"
echo "PROJECT_NAME=$PROJECT_NAME"

# Activate virtual environment and run Dagster
source venv/bin/activate
dagster dev