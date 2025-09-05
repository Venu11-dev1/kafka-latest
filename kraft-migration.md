# Kafka ZooKeeper to KRaft Migration Guide

## Changes Made

### 1. Kafka Configuration Updated to KRaft Mode
- **Removed ZooKeeper service** completely
- **Updated all Kafka brokers** to run in KRaft mode with combined broker/controller roles
- **Added KRaft-specific environment variables**:
  - `KAFKA_NODE_ID`: Unique identifier for each node
  - `KAFKA_PROCESS_ROLES`: Set to "broker,controller" 
  - `KAFKA_CONTROLLER_QUORUM_VOTERS`: Defines the controller quorum
  - `KAFKA_CONTROLLER_LISTENER_NAMES`: Specifies controller listener
  - `CLUSTER_ID`: Shared cluster identifier

### 2. Kafka UI Authentication Added
- **Added login form authentication** with:
  - Username: `admin`
  - Password: `admin123`
- **Removed ZooKeeper references** from Kafka UI configuration

## Migration Steps

### 1. Clean Up Existing Setup
```bash
# Stop and remove existing containers and volumes
docker-compose down -v
docker volume prune -f
```

### 2. Start New KRaft Setup
```bash
# Start the new KRaft-based setup
docker-compose up -d
```

### 3. Verify Setup
```bash
# Check if all containers are running
docker-compose ps

# Check Kafka broker logs
docker-compose logs kafka-broker-1
docker-compose logs kafka-broker-2
docker-compose logs kafka-broker-3

# Check Kafka UI logs
docker-compose logs kafka-ui
```

### 4. Access Kafka UI
- URL: http://localhost:8080
- Username: `admin`
- Password: `admin123`

## Key Benefits of KRaft Mode

1. **Simplified Architecture**: No separate ZooKeeper cluster needed
2. **Better Performance**: Reduced latency and improved throughput
3. **Easier Management**: Single cluster to manage instead of Kafka + ZooKeeper
4. **Better Scalability**: More efficient metadata handling

## Important Notes

- **Data Loss Warning**: This migration will result in complete data loss as we're removing ZooKeeper volumes
- **Production Migration**: For production systems, use Kafka's official migration tools
- **Cluster ID**: The cluster ID `MkU3OEVBNTcwNTJENDM2Qk` is hardcoded - generate a unique one for production
- **Authentication**: Change the default Kafka UI credentials for production use

## Troubleshooting

If containers fail to start:
1. Check logs: `docker-compose logs <service-name>`
2. Ensure all volumes are clean: `docker volume ls` and remove kafka-related volumes
3. Verify network connectivity between brokers
4. Check that ports are not in use by other services