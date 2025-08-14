# Enterprise Dagster Asset Management System

An enterprise-level asset management system built with Dagster, featuring automated discovery, health monitoring, and centralized governance.

## Features

### Asset Management
- **Automated Asset Discovery**: Automatically discovers and registers assets
- **Centralized Metadata Storage**: Stores comprehensive asset metadata
- **Flexible Update Intervals**: Configurable asset update frequencies
- **System Information Tracking**: Captures server, host, and environment details

### Monitoring & Governance
- **Health Monitoring**: Automated health checks for all assets
- **Alert System**: Configurable alerting for asset issues
- **Governance Dashboard**: Centralized dashboard for asset governance
- **Lineage Tracking**: Automatic asset lineage and dependency tracking

### Enterprise Features
- **Low Maintenance**: Smart update mechanisms reduce unnecessary operations
- **Scalable Architecture**: Modular design for horizontal scaling
- **Docker Support**: Containerized deployment for easy scaling
- **Environment Awareness**: Automatic environment detection and tagging

## Project Structure

```
my_dagster_project/
├── assets/                 # Asset definitions
│   ├── ingestion/          # Data ingestion assets
│   ├── loaders/            # Data loading assets
│   └── test_asset.py       # Test asset example
├── core/                   # Core system components
│   ├── asset_discovery.py  # Asset discovery system
│   ├── asset_record.py     # Asset record management
│   ├── config_manager.py   # Configuration management
│   ├── health_monitor.py   # Health monitoring system
│   ├── job_scheduler.py    # Automated job scheduling
│   ├── metadata_store.py   # Metadata storage
│   └── monitoring.py       # Monitoring system
├── governance/             # Governance components
│   └── dashboard.py        # Governance dashboard
├── jobs/                   # Job definitions
│   ├── automated_jobs.py   # Automated maintenance jobs
│   └── data_ingestion_job.py # Data ingestion job
├── schedules/              # Schedule definitions
├── scripts/                # Utility scripts
│   ├── asset_manager.py    # CLI asset management tool
│   ├── initialize.py        # System initialization
│   └── init_db.py          # Database initialization
├── sensors/                # Sensor definitions
│   └── asset_discovery_sensor.py # Asset discovery sensor
├── shared/                 # Shared utilities
│   └── system_info.py      # System information utilities
└── definitions.py          # Dagster definitions
```

## Quick Start

### Development Setup

1. **Clone the repository**:
   ```bash
   git clone <repository-url>
   cd dagster-project
   ```

2. **Create virtual environment**:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

4. **Initialize the system**:
   ```bash
   python my_dagster_project/scripts/initialize.py
   ```

5. **Start Dagster development server**:
   ```bash
   dagster dev
   ```

### Docker Setup

1. **Build and start with Docker Compose**:
   ```bash
   docker-compose up --build
   ```

2. **Access the Dagster UI at http://localhost:3000**

## Configuration

### Environment Variables
- `ENVIRONMENT`: Deployment environment (development, staging, production)
- `PROJECT_NAME`: Project identifier
- `INIT_DB`: Whether to initialize database (true/false)

### Configuration Files
- `config/app_config.yaml`: Main application configuration
- `config/dagster.yaml`: Dagster storage configuration
- `config/workspace.yaml`: Dagster workspace configuration

## CLI Commands

The project includes a comprehensive CLI tool for asset management:

```bash
# Initialize the system
python my_dagster_project/scripts/asset_manager.py init

# List all registered assets
python my_dagster_project/scripts/asset_manager.py list-assets

# Check asset health
python my_dagster_project/scripts/asset_manager.py asset-health

# View asset details
python my_dagster_project/scripts/asset_manager.py asset-details <asset-key>

# Discover and register assets
python my_dagster_project/scripts/asset_manager.py discover-assets

# Generate asset inventory report
python my_dagster_project/scripts/asset_manager.py asset-inventory

# Generate ownership report
python my_dagster_project/scripts/asset_manager.py ownership-report
```

## Docker Deployment

### Development
```bash
docker-compose up --build
```

### Production
```bash
docker-compose -f docker-compose.prod.yml up --build
```

## Key Features Explained

### Asset Update Frequency
Assets are updated based on configurable intervals:
- Default: Every 15 minutes (900 seconds)
- Configurable in `config/app_config.yaml`
- Smart update mechanism: Only updates when interval has passed

### System Information Capture
Each asset automatically captures:
- Server information (`server_name`, `host_name`, `machine_name`)
- User information (`logged_user_name`)
- System details (`operating_system`, `os_version`, `processor`)
- Environment (`environment`, `project_name`)
- Container information (when running in Docker)

### Health Monitoring
- Automated health checks every 15 minutes
- Execution status monitoring
- Data volume anomaly detection
- Execution time performance tracking
- Alert generation for issues

### Automated Jobs
- **Asset Discovery**: Automatically discovers new assets
- **Health Checks**: Regular asset health monitoring
- **Alert Processing**: Processes and notifies on alerts

## Extending the System

### Adding New Assets
1. Create asset definition in `my_dagster_project/assets/`
2. Assets are automatically discovered and registered
3. System information is automatically captured

### Customizing Monitoring
1. Modify thresholds in `config/app_config.yaml`
2. Add custom health checks in `core/health_monitor.py`
3. Create custom alerts in asset definitions

### Adding New Jobs
1. Define job functions in `jobs/automated_jobs.py`
2. Register jobs in `core/job_scheduler.py`
3. Configure schedules in `config/app_config.yaml`

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a pull request