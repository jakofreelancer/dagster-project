# Enterprise Dagster Asset Management System

An enterprise-level asset management system built with Dagster, featuring automated discovery, health monitoring, and centralized governance.

## System Architecture

### Overview
```
┌─────────────────────────────────────────────────────────────────────┐
│                        Docker Container                             │
│  ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐ │
│  │   Dagster UI    │    │ Dagster Daemon  │    │   Scheduler     │ │
│  │  (Web Server)   │◄──►│   (Services)    │◄──►│   (Jobs)        │ │
│  └─────────────────┘    └─────────────────┘    └─────────────────┘ │
│           │                         │                     │        │
│           ▼                         ▼                     ▼        │
│  ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐ │
│  │ Asset Discovery │    │ Health Monitor  │    │ Alert System    │ │
│  │    Service      │    │    Service      │    │    Service      │ │
│  └─────────────────┘    └─────────────────┘    └─────────────────┘ │
│           │                         │                     │        │
│           ▼                         ▼                     ▼        │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │                    SQLite Databases                         │   │
│  │  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐           │   │
│  │  │ metadata.db │ │ monitor.db  │ │ health.db   │           │   │
│  │  └─────────────┘ └─────────────┘ └─────────────┘           │   │
│  │  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐           │   │
│  │  │schedule.db  │ │ assets.db   │ │ (per asset) │           │   │
│  │  └─────────────┘ └─────────────┘ └─────────────┘           │   │
│  └─────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
                    ┌───────────────────┐
                    │   Data Sources    │
                    │ (SQL, APIs, etc.) │
                    └───────────────────┘
```

### Core Components

1. **Dagster Core**: Orchestrates asset pipelines and workflows
2. **Asset Management System**: 
   - Automated discovery and registration
   - Centralized metadata storage
   - Health monitoring and alerting
3. **Database Layer**: SQLite databases for metadata, monitoring, and health data
4. **CLI Tools**: Command-line interface for system management
5. **Docker Environment**: Containerized deployment for consistency

### Data Flow
1. **Asset Discovery**: System automatically scans asset definitions
2. **Registration**: Assets are registered with system metadata
3. **Execution**: Dagster schedules and runs asset jobs
4. **Monitoring**: Health checks and metrics collection
5. **Alerting**: Notifications for issues and anomalies
6. **Governance**: Centralized dashboard for asset oversight

## Quick Start Guide

### Prerequisites
- Python 3.8+
- Docker and Docker Compose (optional, for containerized deployment)
- Git

### Clone the Repository
```bash
git clone <repository-url>
cd dagster-project
```

### Setup Development Environment

#### Option 1: Local Development Setup
```bash
# Create virtual environment
python -m venv venv

# Activate virtual environment
# On Windows:
venv\Scripts\activate
# On macOS/Linux:
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Initialize the system
python my_dagster_project/scripts/initialize.py
```

#### Option 2: Docker Setup
```bash
# Build and start with Docker Compose
docker-compose up --build

# Access the Dagster UI at http://localhost:3000
```

### Running the System

#### Local Development
```bash
# Start Dagster development server
dagster dev

# Access the UI at http://localhost:3000
```

#### Docker Deployment
```bash
# Development environment
docker-compose up

# Production environment
docker-compose -f docker-compose.prod.yml up
```

## Development Workflow

### Creating New Assets

#### 1. Ingestion Asset
Create a new file in `my_dagster_project/assets/ingestion/`:

```python
# my_dagster_project/assets/ingestion/my_new_data_ingestion.py
import pandas as pd
from dagster import asset
from my_dagster_project.shared.load_window import get_load_window
from my_dagster_project.shared.ingest_utils import ingest_from_sql

@asset(
    name="raw_my_new_data",
    group_name="stg__ingestion",
    description="Raw my new data for SQL staging.",
    compute_kind="pandas",
    owners=["your.email@company.com"],
    tags={"domain": "my_domain", "source": "MySource", "type": "ingestion"},
    metadata={
        "source_system": "My Source System",
        "target_schema": "stg",
        "load_window": "dynamic via get_load_window()",
        "owner": "your_username",
        "team": "Your Team"
    }
)
def raw_my_new_data(context) -> pd.DataFrame:
    '''Ingests raw my new data from source system.'''
    
    query = '''
        SELECT * FROM my_source_table
        WHERE created_date >= ? AND created_date < ?
    '''
    
    start, end = get_load_window()
    
    df = ingest_from_sql(
        sql_server='MY_SERVER',
        sql_database='MY_DATABASE',
        query=query,
        date_params=[start, end]
    )
    
    # Add Dagster UI metadata
    context.add_output_metadata({
        "row_count": len(df),
        "window_start": str(start),
        "window_end": str(end),
        "preview": df.head().to_markdown() if not df.empty else "No data"
    })
    
    return df

# Register this asset in the all_assets list
all_assets = [raw_my_new_data]
```

#### 2. Loading Asset
Create a new file in `my_dagster_project/assets/loaders/`:

```python
# my_dagster_project/assets/loaders/my_new_data_loader.py
from dagster import AssetExecutionContext, asset, AssetIn
from my_dagster_project.shared.load_to_sql_utils import load_to_stg_table, sanitize_dataframe
from pandas import DataFrame

@asset(
    name="stg_my_new_data",
    ins={"raw_my_new_data": AssetIn()},
    group_name="stg__loaders",
    description="Loads raw my new data into staging table.",
    compute_kind="pandas",
    owners=["your.email@company.com"],
    tags={"domain": "my_domain", "source": "MySource", "type": "loader"},
    metadata={
        "target_table": "my_new_data_staging",
        "target_schema": "stg"
    }
)
def stg_my_new_data(
    context: AssetExecutionContext,
    raw_my_new_data: DataFrame
):
    '''Loads the raw my new data into a preparation table.'''
    
    sanitized_data = sanitize_dataframe(raw_my_new_data)
    
    load_to_stg_table(
        context=context,
        df=sanitized_data,
        target_table_name='my_new_data_staging'
    )

# Register this asset in the all_assets list
all_assets = [stg_my_new_data]
```

#### 3. Transformation Asset
Create a new file in `my_dagster_project/assets/`:

```python
# my_dagster_project/assets/my_new_data_transform.py
import pandas as pd
from dagster import AssetExecutionContext, asset, AssetIn

@asset(
    name="transformed_my_new_data",
    ins={
        "stg_my_new_data": AssetIn(),
        # Add other dependencies as needed
    },
    group_name="transformed_data",
    description="Transforms staging data into final format.",
    compute_kind="pandas",
    owners=["your.email@company.com"],
    tags={"domain": "my_domain", "type": "transformation"},
    metadata={
        "business_rules": "Apply business rules X, Y, Z",
        "output_format": "Standardized format for analytics"
    }
)
def transformed_my_new_data(
    context: AssetExecutionContext,
    stg_my_new_data: pd.DataFrame
) -> pd.DataFrame:
    '''Transforms staging data into final format for analytics.'''
    
    # Apply transformations
    transformed_df = stg_my_new_data.copy()
    
    # Example transformations
    transformed_df['processed_date'] = pd.Timestamp.now()
    transformed_df['record_count'] = len(transformed_df)
    
    # Add business logic here
    # ...
    
    # Log metrics
    context.add_output_metadata({
        "row_count": len(transformed_df),
        "preview": transformed_df.head().to_markdown() if not transformed_df.empty else "No data"
    })
    
    return transformed_df

# Register this asset
all_assets = [transformed_my_new_data]
```

### Registering New Assets

#### Automatic Registration
The system automatically discovers new assets:
1. Place asset files in the appropriate directories
2. Ensure they follow the naming convention with `all_assets` list
3. The asset discovery service will find and register them automatically

#### Manual Registration
```bash
# Discover and register new assets manually
python my_dagster_project/scripts/asset_manager.py discover-assets

# Verify registration
python my_dagster_project/scripts/asset_manager.py list-assets
```

### Updating Existing Assets

#### 1. Code Changes
Simply modify the asset code in its Python file. The system will:
1. Automatically detect changes during discovery
2. Update metadata in the central registry
3. Maintain execution history

#### 2. Manual Update Command
```bash
# Force asset discovery and update
python my_dagster_project/scripts/asset_manager.py discover-assets

# View updated asset details
python my_dagster_project/scripts/asset_manager.py asset-details asset_key
```

### Schema Change Management

#### Handling Source Schema Changes
When source schemas change:

1. **Update the asset code** to handle new/removed columns:
```python
@asset
def raw_my_data(context) -> pd.DataFrame:
    # Handle schema changes gracefully
    df = ingest_from_sql(...)
    
    # Add default values for new columns
    if 'new_column' not in df.columns:
        df['new_column'] = None
    
    # Handle removed columns
    expected_columns = ['col1', 'col2', 'new_column']
    for col in expected_columns:
        if col not in df.columns:
            df[col] = None
    
    return df
```

2. **Update dependencies** that rely on the changed schema:
```bash
# Run discovery to update lineage
python my_dagster_project/scripts/asset_manager.py discover-assets

# Check asset health to identify issues
python my_dagster_project/scripts/asset_manager.py asset-health
```

#### Schema Evolution Best Practices
1. **Backward Compatibility**: Maintain compatibility with existing consumers
2. **Versioning**: Use asset versioning for major changes
3. **Documentation**: Update asset metadata with schema change notes
4. **Testing**: Test downstream assets after schema changes

### Asset Management Commands

```bash
# Initialize the system
python my_dagster_project/scripts/asset_manager.py init

# List all registered assets
python my_dagster_project/scripts/asset_manager.py list-assets

# Check asset health
python my_dagster_project/scripts/asset_manager.py asset-health

# View detailed asset information
python my_dagster_project/scripts/asset_manager.py asset-details <asset-key>

# Discover and register new assets
python my_dagster_project/scripts/asset_manager.py discover-assets

# Generate asset inventory report
python my_dagster_project/scripts/asset_manager.py asset-inventory

# Generate ownership report
python my_dagster_project/scripts/asset_manager.py ownership-report

# Simulate asset execution (for testing)
python my_dagster_project/scripts/simulate_execution.py
```

## Configuration Management

### Environment Variables
```bash
# Set environment (development, staging, production)
ENVIRONMENT=development

# Set project name
PROJECT_NAME=my-dagster-project

# Set Dagster home directory
DAGSTER_HOME=/opt/dagster/dagster_home
```

### Configuration Files
- `config/app_config.yaml`: Main application settings
- `config/dagster.yaml`: Dagster storage configuration
- `config/workspace.yaml`: Dagster workspace setup

## Monitoring and Alerting

### Health Checks
The system performs automated health checks:
- Execution status monitoring
- Data volume anomaly detection
- Performance timing analysis
- Dependency validation

### Alert Management
```bash
# View active alerts
python my_dagster_project/scripts/asset_manager.py alerts

# Resolve specific alerts (in code)
# Alerts resolve automatically when issues are fixed
```

## Troubleshooting

### Common Issues

#### 1. Asset Not Discovered
```bash
# Check asset discovery
python my_dagster_project/scripts/asset_manager.py discover-assets --verbose

# Verify file structure and naming
```

#### 2. Database Connection Issues
```bash
# Check database permissions
ls -la /opt/dagster/dagster_home/

# Verify Dagster home is set
echo $DAGSTER_HOME
```

#### 3. Docker Issues
```bash
# Rebuild Docker images
docker-compose build --no-cache

# Check container logs
docker-compose logs dagster
```

### System Maintenance

#### Database Maintenance
```bash
# Clean up old execution data (automated)
# Configured in app_config.yaml

# Backup databases
# Part of production deployment process
```

#### Log Management
```bash
# View system logs
tail -f logs/system.log

# Rotate logs
# Handled automatically by Docker logging
```

## Production Deployment

### Docker Deployment
```bash
# Build production images
docker-compose -f docker-compose.prod.yml build

# Start production services
docker-compose -f docker-compose.prod.yml up -d

# Scale services if needed
docker-compose -f docker-compose.prod.yml up -d --scale dagster-worker=3
```

### Environment-Specific Configuration
1. Set environment variables for each deployment
2. Use different configuration files per environment
3. Configure resource limits in docker-compose

## Contributing

### Development Process
1. Fork the repository
2. Create feature branch
3. Implement changes
4. Test thoroughly
5. Submit pull request

### Code Standards
- Follow existing code patterns
- Add comprehensive documentation
- Include unit tests for new functionality
- Update README when adding new features

### Testing
```bash
# Run unit tests
pytest my_dagster_project_tests

# Test asset discovery
python my_dagster_project/scripts/asset_manager.py discover-assets --test

# Validate configuration
python my_dagster_project/scripts/validate_config.py
```

## License

This project is licensed under the MIT License - see the LICENSE file for details.