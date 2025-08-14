# Dagster Project with Docker

This project includes Docker configuration for running your Dagster application in containers.

## Development Setup

### Quick Start

1. Build and start the services:
   ```bash
   docker-compose up --build
   ```

2. Access the Dagster UI at http://localhost:3000

### Development Commands

```bash
# Build the containers
docker-compose build

# Start services in detached mode
docker-compose up -d

# View logs
docker-compose logs -f

# Stop services
docker-compose down

# Stop services and remove volumes
docker-compose down -v
```

## Production Setup

### Quick Start

1. Build and start the production services:
   ```bash
   docker-compose -f docker-compose.prod.yml up --build
   ```

2. Access the Dagster UI at http://localhost:3000

### Production Commands

```bash
# Build the production containers
docker-compose -f docker-compose.prod.yml build

# Start production services in detached mode
docker-compose -f docker-compose.prod.yml up -d

# View logs
docker-compose -f docker-compose.prod.yml logs -f

# Stop services
docker-compose -f docker-compose.prod.yml down

# Stop services and remove volumes
docker-compose -f docker-compose.prod.yml down -v
```

## Environment Variables

The following environment variables can be set:

- `ENVIRONMENT`: deployment environment (development, staging, production)
- `PROJECT_NAME`: name of the project
- `DAGSTER_HOME`: path to Dagster home directory

## Database

The project uses SQLite for storing Dagster metadata. The databases are automatically created when first accessed and stored in the Dagster home directory:
- `metadata.db`: Asset metadata storage
- `monitoring.db`: Monitoring data
- `health.db`: Health check results
- `scheduler.db`: Job scheduling information
- `assets.db`: Asset record information

## Volumes

The following volumes are used:

- `dagster_home`: Persistent storage for Dagster home directory and databases
- `./logs`: Logs directory mounted from host

## Customization

To customize the Docker setup:

1. Modify `Dockerfile` for development or `Dockerfile.prod` for production
2. Update `docker-compose.yml` or `docker-compose.prod.yml` as needed
3. Adjust environment variables in the docker-compose files

## Troubleshooting

### Common Docker Issues

1. **Permission errors**: Ensure Docker has access to project directory
2. **Port conflicts**: Change ports in docker-compose files if 3000 is in use
3. **Volume issues**: Check Docker volume permissions

### Debugging

```bash
# Check container status
docker-compose ps

# View container logs
docker-compose logs dagster

# Access container shell
docker-compose exec dagster bash
```