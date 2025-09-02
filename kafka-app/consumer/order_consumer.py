from confluent_kafka import Consumer
import json
import smtplib
from email.mime.text import MIMEText
import psycopg2

from config.settings import (
    BROKER, TOPIC,
    SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASS, MAIL_FROM,
    DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS,
    ENABLE_ORDER_CONSUMER
)
from utils.logger import log

if ENABLE_ORDER_CONSUMER == "false":
    log("Order consumer disabled by config.", "INFO")
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

# Send confirmation email
def send_email(to_email, order_number):
    try:
        msg = MIMEText(f"Your order {order_number} has been received and is being processed.")
        msg["Subject"] = f"Order Confirmation - {order_number}"
        msg["From"] = MAIL_FROM
        msg["To"] = to_email

        server = smtplib.SMTP(SMTP_HOST, SMTP_PORT)
        server.starttls()
        server.login(SMTP_USER, SMTP_PASS)
        server.sendmail(MAIL_FROM, [to_email], msg.as_string())
        server.quit()

        log(f"Email sent to {to_email} for order {order_number}", "INFO")
    except Exception as e:
        log(f"Email failed: {e}", "ERROR")

# Update order status in DB
def update_order_status(order_id, new_status="processing"):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("UPDATE orders SET status = %s, updated_at = NOW() WHERE id = %s", (new_status, order_id))       
        conn.commit()
        cur.close()
        conn.close()
        log(f"DB updated: Order {order_id} status -> {new_status}", "INFO")
    except Exception as e:
        log(f"DB update failed for Order {order_id}: {e}", "ERROR")

# Consume messages from Kafka
def consume_orders():
    consumer = Consumer({
        'bootstrap.servers': BROKER,
        'group.id': 'orders-consumer-group',
        'auto.offset.reset': 'earliest'
    })

    consumer.subscribe([TOPIC])
    log(f"Listening for orders on topic '{TOPIC}'...", "INFO")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue
            elif msg.error():
                log(f"Kafka error: {msg.error()}", "ERROR")
            else:
                order = json.loads(msg.value().decode('utf-8'))
                log(f"Processing Order #{order['order_number']} for {order['email']}", "INFO")

                send_email(order['email'], order['order_number'])
                update_order_status(order['id'])

    except KeyboardInterrupt:
        log("Stopped by user.", "INFO")

    finally:
        consumer.close()

if __name__ == "__main__":
    consume_orders()
