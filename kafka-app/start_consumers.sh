#!/bin/bash

# Start 3 consumer instances for parallel processing
echo "Starting 3 consumer instances..."

# Start consumers in background using module syntax
python3 -m consumer.email_consumer_latest &
python3 -m consumer.email_consumer_latest &
python3 -m consumer.email_consumer_latest &

echo "3 consumers started. Processing orders from 3 partitions in parallel."

# Keep container running and show logs
tail -f /dev/null