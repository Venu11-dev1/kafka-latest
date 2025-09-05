#!/bin/bash

echo "========================================="
echo "🚀 KAFKA CLUSTER SETUP SCRIPT"
echo "========================================="

# Start Kafka cluster
echo "Starting Kafka cluster with Docker Compose..."
docker-compose up -d

# Wait for Kafka to be ready
echo "Waiting for Kafka brokers to be ready..."
sleep 15

# Create topic with 3 partitions and replication factor of 3
echo "Creating 'orders' topic with 3 partitions and replication factor 3..."
docker exec kafka-broker-1 kafka-topics --create \
    --bootstrap-server kafka-broker-1:29092 \
    --topic orders \
    --partitions 3 \
    --replication-factor 3 \
    --if-not-exists

# Verify topic creation
echo "Verifying topic configuration..."
docker exec kafka-broker-1 kafka-topics --describe \
    --bootstrap-server kafka-broker-1:29092 \
    --topic orders

echo "========================================="
echo "✅ KAFKA SETUP COMPLETE"
echo "========================================="
echo ""
echo "Access points:"
echo "  → Broker 1: localhost:9092"
echo "  → Broker 2: localhost:9093"
echo "  → Broker 3: localhost:9094"
echo "  → Kafka UI: http://localhost:8080"
echo ""
echo "To run the producer (Laravel):"
echo "  php artisan app:send-orders-to-kafka"
echo ""
echo "To run the consumer (Python):"
echo "  python consumer/email_consumer_latest.py"
echo ""
echo "To monitor logs:"
echo "  docker-compose logs -f"
echo "========================================="