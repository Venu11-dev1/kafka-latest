"""
delete_topic.py
---------------
Deletes one or more Kafka topics using AdminClient.
"""

from .admin_client import admin_client
from utils.logger import log

# 🗑️ Function to delete topics
def delete_topic(topics):
    if isinstance(topics, str):
        topics = [topics]  # Convert single topic to list

    try:
        futures = admin_client.delete_topics(topics, operation_timeout=30)

        for topic, future in futures.items():
            future.result()
            log(f"🗑️ Topic '{topic}' deleted successfully")
    except Exception as e:
        log(f"❌ Failed to delete topic(s): {e}", "ERROR")


# 🧪 Run this file standalone to delete demo topics
if __name__ == "__main__":
    log("🗑️ Deleting demo topics...")
    delete_topic(["demo-topic-1", "demo-topic-2"])
