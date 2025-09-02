from .admin_client import admin_client
from utils.logger import log

def delete_all_topics():
    # List all topics
    metadata = admin_client.list_topics(timeout=10)
    topics = list(metadata.topics.keys())

    if not topics:
        log("⚠️  No topics found.", "WARNING")
        return

    log(f"🧹 Deleting topics: {topics}")
    fs = admin_client.delete_topics(topics, operation_timeout=30)

    for topic, f in fs.items():
        try:
            f.result()
            log(f"✅ Deleted topic: {topic}")
        except Exception as e:
            log(f"❌ Failed to delete topic {topic}: {e}", "ERROR")

if __name__ == "__main__":
    delete_all_topics()
