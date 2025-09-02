from confluent_kafka import Consumer
import json, psycopg2
from utils.logger import log
from config.settings import (
    BROKER, 
    TOPIC, 
    DB_HOST, 
    DB_PORT, 
    DB_NAME, 
    DB_USER, 
    DB_PASS,
    ENABLE_ORDER_DB_CONSUMER
)

if ENABLE_ORDER_DB_CONSUMER == "false":
    log("🚫 DB consumer disabled by config.", "WARNING")
    exit(0)


# Setup PostgreSQL connection
def get_db_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )

def update_order_status(order_id, new_status="processing"):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(
            "UPDATE orders SET status = %s, updated_at = NOW() WHERE id = %s",
            (new_status, order_id)
        )
        conn.commit()
        cur.close()
        conn.close()
        log(f"✅ DB updated: Order {order_id} -> {new_status}")
    except Exception as e:
        log(f"❌ DB update failed for Order {order_id}: {e}", "ERROR")

def consume_db_updates():
    consumer = Consumer({
        'bootstrap.servers': BROKER,
        'group.id': 'orders-db-group',
        'auto.offset.reset': 'earliest'
    })
    consumer.subscribe([TOPIC])
    log(f"📥 DB Consumer listening on '{TOPIC}'...")
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            elif msg.error():
                log(f"Kafka error: {msg.error()}", "ERROR")
            else:
                order = json.loads(msg.value().decode('utf-8'))
                update_order_status(order['id'])
    except KeyboardInterrupt:
        log("Stopping DB consumer...")
    finally:
        consumer.close()

if __name__ == "__main__":
    consume_db_updates()
