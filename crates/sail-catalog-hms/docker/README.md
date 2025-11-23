# HMS Docker Test Environment

This directory contains a Docker Compose setup for testing HMS catalog provider against multiple Hive Metastore versions.

## Services

The docker-compose.yml provides the following services:

1. **PostgreSQL** (port 5432): Backend database for HMS metadata
2. **HMS 2.3.9** (port 9083): Legacy HMS version
3. **HMS 3.1.3** (port 9084): Current stable HMS version
4. **HMS 4.0.0** (port 9085): Latest HMS version
5. **MinIO** (ports 9000, 9001): Object storage for external tables (optional)

## Usage

### Start All Services

```bash
docker-compose up -d
```

### Start Specific HMS Version

```bash
# Start only HMS 3.1
docker-compose up -d postgres hms-3.1

# Start only HMS 4.0
docker-compose up -d postgres hms-4.0
```

### Check Service Status

```bash
docker-compose ps
```

### View Logs

```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f hms-3.1
```

### Stop Services

```bash
docker-compose down
```

### Clean Up (Including Volumes)

```bash
docker-compose down -v
```

## Connecting to HMS

### From Host Machine

Each HMS instance is exposed on a different port:

- **HMS 2.3**: `thrift://localhost:9083`
- **HMS 3.1**: `thrift://localhost:9084`
- **HMS 4.0**: `thrift://localhost:9085`

### From Docker Network

Within the Docker network, use:

- **HMS 2.3**: `thrift://hms-2.3:9083`
- **HMS 3.1**: `thrift://hms-3.1:9083`
- **HMS 4.0**: `thrift://hms-4.0:9083`

## Testing

### Manual Testing with Beeline (Hive CLI)

```bash
# Connect to HMS 3.1 container
docker exec -it hms-3.1 beeline -u "jdbc:hive2://localhost:10000"

# Run some commands
CREATE DATABASE test_db;
USE test_db;
CREATE TABLE test_table (id INT, name STRING);
SHOW TABLES;
```

### Integration Tests

```bash
# Run integration tests against HMS 3.1
export HMS_URI=thrift://localhost:9084
cargo test --package sail-catalog-hms --features integration-tests

# Run against HMS 4.0
export HMS_URI=thrift://localhost:9085
cargo test --package sail-catalog-hms --features integration-tests
```

## MinIO Object Storage

MinIO is provided for testing external tables with S3-compatible storage.

- **API Endpoint**: http://localhost:9000
- **Console**: http://localhost:9001
- **Username**: minioadmin
- **Password**: minioadmin

### Create a bucket for testing

```bash
# Using AWS CLI
aws --endpoint-url http://localhost:9000 \
    s3 mb s3://warehouse

# Or use the MinIO Console at http://localhost:9001
```

## Troubleshooting

### HMS Container Won't Start

Check if PostgreSQL is healthy:

```bash
docker-compose ps postgres
```

View HMS logs:

```bash
docker-compose logs hms-3.1
```

### Connection Refused

Wait for HMS to fully initialize (can take 30-60 seconds):

```bash
# Wait for health check
docker-compose ps

# Check if HMS is ready
docker-compose logs hms-3.1 | grep "Started"
```

### Port Already in Use

If ports are already in use, modify the port mappings in docker-compose.yml:

```yaml
ports:
  - "19083:9083"  # Change to an available port
```

### Reset Everything

```bash
# Stop and remove containers, networks, volumes
docker-compose down -v

# Restart fresh
docker-compose up -d
```

## Performance Tips

### Reduce Memory Usage

If running all HMS versions is too resource-intensive, start only what you need:

```bash
# Just HMS 3.1 for development
docker-compose up -d postgres hms-3.1
```

### Persistent Data

Data is stored in Docker volumes:
- `postgres-data`: Database metadata
- `warehouse-{version}`: Table data for each HMS version

To preserve data across restarts, don't use the `-v` flag when running `docker-compose down`.

## Configuration

### Custom HMS Configuration

Mount custom `hive-site.xml`:

```yaml
volumes:
  - ./custom-hive-site.xml:/opt/hive/conf/hive-site.xml
```

### Environment Variables

Modify in docker-compose.yml:

```yaml
environment:
  SERVICE_OPTS: >-
    -Dhive.metastore.warehouse.dir=/custom/warehouse
    -Dhive.metastore.schema.verification=false
```

## Version Information

| Component  | Version  |
|------------|----------|
| PostgreSQL | 15       |
| HMS        | 2.3.9    |
| HMS        | 3.1.3    |
| HMS        | 4.0.0    |
| MinIO      | latest   |
