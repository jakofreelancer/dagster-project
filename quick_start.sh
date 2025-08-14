#!/bin/bash
# Quick Start Script for Dagster Project

echo "========================================="
echo "  Enterprise Dagster Asset Management    "
echo "           Quick Start Guide             "
echo "========================================="

echo
echo "Step 1: Setting up the environment..."
echo "----------------------------------------"
echo "Creating virtual environment..."
python -m venv venv

echo "Activating virtual environment..."
source venv/bin/activate  # On Windows: venv\Scripts\activate

echo "Installing dependencies..."
pip install -r requirements.txt

echo
echo "Step 2: Initializing the system..."
echo "----------------------------------------"
python my_dagster_project/scripts/initialize.py

echo
echo "Step 3: Starting the Dagster server..."
echo "----------------------------------------"
echo "Run the following command to start Dagster:"
echo "  dagster dev"
echo
echo "Then access the UI at http://localhost:3000"

echo
echo "Step 4: Useful commands..."
echo "----------------------------------------"
echo "Discover assets: python my_dagster_project/scripts/asset_manager.py discover-assets"
echo "List assets:     python my_dagster_project/scripts/asset_manager.py list-assets"
echo "Check health:    python my_dagster_project/scripts/asset_manager.py asset-health"

echo
echo "For Docker setup, run:"
echo "  docker-compose up --build"

echo
echo "Documentation: README.md"
echo "Happy coding!"