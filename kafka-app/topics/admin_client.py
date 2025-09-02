"""
Kafka AdminClient API Example
-----------------------------
This script demonstrates how to interact with Kafka using the AdminClient API from confluent-kafka.

✅ Features:
- List all topics
- Create one or more topics with configurable partitions/replication
- Delete one or more topics

🧠 What is AdminClient?
AdminClient is a Kafka API that allows administrative operations like creating, deleting,
and inspecting topics programmatically.

🔧 Environment Variables (from .env):
- KAFKA_BROKER: your Kafka broker address (e.g. localhost:9092)

Author: Venu
"""

from confluent_kafka.admin import AdminClient, NewTopic
from dotenv import load_dotenv
import os
import sys
from config.settings import BROKER
from utils.logger import log

# ❗ Exit if broker not found
if not BROKER:
    log("❌ Error: KAFKA_BROKER not found in .env file.", "ERROR")
    sys.exit(1)

# 🛠️ Initialize AdminClient for Kafka operations
admin_client = AdminClient({'bootstrap.servers': BROKER})

# 🧪 Allow standalone run for broker test
if __name__ == "__main__":
    log(f"✅ Kafka AdminClient initialized with broker: {BROKER}")