"""
list_topic.py
-------------
Lists all topics in the Kafka cluster using AdminClient.
"""

from .admin_client import admin_client

# 📜 Function to list topics
def list_topics():
    try:
        metadata = admin_client.list_topics(timeout=10)
        print("\n📋 Available Topics:")
        for topic in metadata.topics:
            print(f" - {topic}")
    except Exception as e:
        print(f"❌ Error listing topics: {e}")


# 🧪 Run this file standalone to list topics
if __name__ == "__main__":
    print("🔍 Listing Kafka topics...")
    list_topics()
