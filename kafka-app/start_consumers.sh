#!/bin/bash

NUM_CONSUMERS=3

for i in $(seq 1 $NUM_CONSUMERS); do
    echo "Starting consumer $i..."
    python3 -m consumer.email_consumer_latest &
done

echo "✅ $NUM_CONSUMERS consumers started."
tail -f /dev/null


# #!/bin/bash

# # Start 3 consumer instances for parallel processing
# echo "Starting 3 consumer instances..."

# # Start consumers in background using module syntax
# python3 -m consumer.email_consumer_latest &
# python3 -m consumer.email_consumer_latest &
# python3 -m consumer.email_consumer_latest &

# echo "3 consumers started. Processing orders from 3 partitions in parallel."

# # Keep container running and show logs
# tail -f /dev/null