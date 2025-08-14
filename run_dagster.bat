@echo off
REM Script to set environment variables for different deployment environments

REM Set environment variables
set ENVIRONMENT=development
set PROJECT_NAME=dagster-project

REM For production environment, you would use:
REM set ENVIRONMENT=production
REM set PROJECT_NAME=your-production-project-name

REM For staging environment, you would use:
REM set ENVIRONMENT=staging
REM set PROJECT_NAME=your-staging-project-name

echo Environment variables set:
echo ENVIRONMENT=%ENVIRONMENT%
echo PROJECT_NAME=%PROJECT_NAME%

REM Activate virtual environment and run Dagster
call venv\Scripts\activate
dagster dev