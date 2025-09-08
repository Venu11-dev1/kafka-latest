from confluent_kafka import Consumer, TopicPartition, KafkaException, KafkaError
from confluent_kafka.admin import AdminClient

BROKER = "kafka-broker-1:29092,kafka-broker-2:29093,kafka-broker-3:29094"
TOPIC = "order-status"   # 🔁 change this to your topic name

def main():
    try:
        # Create Admin client to fetch metadata
        admin_client = AdminClient({"bootstrap.servers": BROKER})
        metadata = admin_client.list_topics(timeout=10)

        if TOPIC not in metadata.topics:
            print(f"❌ Topic '{TOPIC}' not found in cluster")
            return

        topic_meta = metadata.topics[TOPIC]

        print(f"\n📌 Topic: {TOPIC}")
        for partition in topic_meta.partitions.values():
            print(f"Partition {partition.id} → Leader Broker {partition.leader}")

        # Create a consumer just to check offsets
        consumer = Consumer({
            "bootstrap.servers": BROKER,
            "group.id": "partition-checker",
            "auto.offset.reset": "earliest",
        })

        # Build list of partitions
        partitions = [TopicPartition(TOPIC, p.id) for p in topic_meta.partitions.values()]

        # Query offsets
        beginning = {}
        end = {}
        for tp in partitions:
            low, high = consumer.get_watermark_offsets(tp, timeout=5)
            beginning[tp] = low
            end[tp] = high

        print("\n📊 Partition Message Counts:")
        total = 0
        for tp in partitions:
            count = end[tp] - beginning[tp]
            total += count
            print(f"  Partition {tp.partition}: {count} messages")

        print(f"\n✅ Total messages in topic '{TOPIC}': {total}")

        consumer.close()

    except KafkaException as e:
        print(f"❌ Kafka error: {e}")
    except Exception as ex:
        print(f"❌ Error: {ex}")

if __name__ == "__main__":
    main()
