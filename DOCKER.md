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
- `INIT_DB`: whether to initialize the database (true/false)

## Database

The project uses PostgreSQL for storing Dagster metadata. The database is automatically created and initialized when the services start.

## Volumes

The following volumes are used:

- `dagster_home`: Persistent storage for Dagster home directory
- `postgres_data`: Persistent storage for PostgreSQL data
- `./logs`: Logs directory mounted from host

## Customization

To customize the Docker setup:

1. Modify `Dockerfile` for development or `Dockerfile.prod` for production
2. Update `docker-compose.yml` or `docker-compose.prod.yml` as needed
3. Adjust environment variables in the docker-compose files