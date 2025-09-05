#!/usr/bin/env python3

import json
import logging
import psycopg2
from confluent_kafka import Producer
from config.settings import BROKER, TOPIC, DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s'
)
logger = logging.getLogger(__name__)

def get_db_connection():
    """Get database connection"""
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        return conn
    except Exception as e:
        logger.error(f"❌ Database connection failed: {e}")
        return None

def delivery_report(err, msg):
    """Kafka message delivery callback"""
    if err is not None:
        logger.error(f"❌ Message delivery failed: {err}")
    else:
        logger.info(f"✅ Message delivered to {msg.topic()} [partition: {msg.partition()}, offset: {msg.offset()}]")

def fetch_orders_from_db():
    """Fetch all orders from database"""
    conn = get_db_connection()
    if not conn:
        return []
    
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT id, email, order_number, amount, status, items, created_at, updated_at FROM orders ORDER BY id")
        
        orders = []
        for row in cursor.fetchall():
            order = {
                'id': row[0],
                'email': row[1],
                'order_number': row[2],
                'amount': float(row[3]),
                'status': row[4],
                'items': row[5],
                'created_at': row[6].isoformat() if row[6] else None,
                'updated_at': row[7].isoformat() if row[7] else None
            }
            orders.append(order)
        
        cursor.close()
        conn.close()
        
        logger.info(f"📄 Fetched {len(orders)} orders from database")
        return orders
        
    except Exception as e:
        logger.error(f"❌ Failed to fetch orders from database: {e}")
        if conn:
            conn.close()
        return []

def produce_orders():
    """Produce orders to Kafka topic"""
    
    # Configure Kafka producer
    producer_config = {
        "bootstrap.servers": BROKER,
        "linger.ms": 5,
        "batch.num.messages": 10000,
        "queue.buffering.max.messages": 1000000,
        "acks": "all"
    }
    
    producer = Producer(producer_config)
    
    # Fetch orders from database
    orders = fetch_orders_from_db()
    
    if not orders:
        logger.warning("⚠️ No orders found in database")
        return
    
    logger.info(f"🚀 Starting to produce {len(orders)} orders to topic: {TOPIC}")
    logger.info(f"📡 Using brokers: {BROKER}")
    
    try:
        # Send each order to Kafka
        for order in orders:
            order_json = json.dumps(order)
            
            # Use order_number as key for partitioning
            key = order['order_number'].encode('utf-8')
            
            producer.produce(
                TOPIC,
                key=key,
                value=order_json,
                callback=delivery_report
            )
            
            # Trigger delivery reports
            producer.poll(0)
        
        # Wait for all messages to be delivered
        logger.info("⏳ Waiting for all messages to be delivered...")
        producer.flush()
        
        logger.info(f"✅ Successfully produced {len(orders)} orders to Kafka!")
        
        # Show distribution across brokers
        logger.info("📊 Orders distributed across partitions for load balancing")
        
    except Exception as e:
        logger.error(f"❌ Failed to produce orders: {e}")
    finally:
        producer.flush()

if __name__ == "__main__":
    produce_orders()