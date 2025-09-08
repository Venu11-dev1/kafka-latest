#!/bin/bash

echo "🚀 Starting 3 email consumer instances for parallel processing..."

# Start 3 consumers in background
python3 -m consumer.email_consumer_latest &
python3 -m consumer.email_consumer_latest &
python3 -m consumer.email_consumer_latest &

echo "✅ 3 consumers started. Each will process orders from Kafka partitions in parallel."

# Keep the container alive so consumers keep running
tail -f /dev/null
