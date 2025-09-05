# Kafka Orders Processing System

## Architecture
- **3 Kafka Brokers** with replication factor 3
- **3 Partitions** for the 'orders' topic
- **3 Consumer instances** processing emails in parallel
- **Laravel Producer** sending 90 orders distributed across partitions
- **Python Consumers** processing ~30 orders each

## Quick Start

### 1. Start the Kafka Cluster
```bash
docker-compose up -d
```

### 2. Create the Topic (if not auto-created)
```bash
docker exec kafka-broker-1 kafka-topics --create \
    --bootstrap-server kafka-broker-1:29092 \
    --topic orders \
    --partitions 3 \
    --replication-factor 3
```

### 3. Run the Producer (Laravel)
From your Laravel project:
```bash
php artisan app:send-orders-to-kafka
```

### 4. Run the Consumer (Python)
Inside the kafka-app container:
```bash
# Enter the container
docker exec -it kafka-app bash

# Run the consumer
python consumer/email_consumer_latest.py

# Or run multiple consumers (3 instances)
python consumer/email_consumer_latest.py &
python consumer/email_consumer_latest.py &
python consumer/email_consumer_latest.py &
```

## Access Points
- **Broker 1**: localhost:9092
- **Broker 2**: localhost:9093  
- **Broker 3**: localhost:9094
- **Kafka UI**: http://localhost:8080

## Environment Variables
Update these in docker-compose.yml:
- `SMTP_USER`: Your email address
- `SMTP_PASS`: Your app password
- `MAIL_FROM`: Sender email address

## Monitoring
- View Kafka UI: http://localhost:8080
- Check logs: `docker-compose logs -f kafka-app`
- Producer logs: Check Laravel logs
- Consumer logs: `/logs` directory

## Flow
1. Laravel sends 90 orders → Kafka (round-robin to partitions)
2. Each partition gets ~30 orders
3. 3 consumer instances process in parallel (one per partition)
4. Emails sent for each order
5. All operations logged with partition/offset details