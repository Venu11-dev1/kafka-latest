"""
create_topic.py
---------------
Creates one or more Kafka topics with specified partitions and replication.
"""

from confluent_kafka.admin import NewTopic
from .admin_client import admin_client
from utils.logger import log

# 🆕 Function to create topics
def create_topic(topics, num_partitions=3, replication_factor=3):
    if isinstance(topics, str):
        topics = [topics]  # Convert single topic to list

  

    topic_objs = []
    for topic in topics:
        topic_objs.append(NewTopic(topic, num_partitions=num_partitions, replication_factor=replication_factor))

      # topic_objs = [
    #     NewTopic(topic, num_partitions=num_partitions, replication_factor=replication_factor)
    #     for topic in topics
    # ]

    try:
        futures = admin_client.create_topics(topic_objs)

        for topic, future in futures.items():
            future.result()  # Wait for result (or catch errors)
            log(f"✅ Topic '{topic}' created with {num_partitions} partitions and replication {replication_factor}")
    except Exception as e:
        log(f"❌ Failed to create topic(s): {e}", "ERROR")


# 🧪 Run this file standalone to create demo topics
if __name__ == "__main__":
    log("🔨 Creating demo topics...")
    # create_topic(["demo-topic-1"], num_partitions=2)
    create_topic(["orders"], num_partitions=3)

