from confluent_kafka.admin import AdminClient
import os
from config.settings import BROKER
GROUP = "laravel-order-status-group"

admin_client = AdminClient({"bootstrap.servers": BROKER})

fs = admin_client.delete_consumer_groups([GROUP])

for group, f in fs.items():
    try:
        f.result()
        print(f"✅ Deleted consumer group: {group}")
    except Exception as e:
        print(f"❌ Failed to delete consumer group {group}: {e}")
