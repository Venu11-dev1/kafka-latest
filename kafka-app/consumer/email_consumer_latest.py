import json
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from confluent_kafka import Consumer, Producer, KafkaError
from config.settings import (
    BROKER,
    TOPIC,
    SMTP_HOST,
    SMTP_PORT,
    SMTP_USER,
    SMTP_PASS,
    MAIL_FROM,
    ENABLE_ORDER_EMAIL_CONSUMER
)
from utils.logger import log  # <-- use custom logger

# -------------------- Producer --------------------
status_producer = Producer({
    'bootstrap.servers': BROKER,
    'api.version.request': True,
    'socket.timeout.ms': 60000
})

def send_status_update(order_id, status):
    """Send order status back to Kafka."""
    event = {
        "order_id": order_id,
        "status": status
    }
    try:
        status_producer.produce(
            topic="order-status",
            value=json.dumps(event).encode('utf-8')
        )
        status_producer.flush()
        log(f"Sent status update for Order {order_id}: {status}", level="INFO", obj=event)
    except Exception as e:
        log(f"Failed to send status for Order {order_id}: {e}", level="ERROR", obj=event)

# -------------------- Email --------------------
def send_order_email(order_data):
    """Send email for a single order."""
    try:
        msg = MIMEMultipart('alternative')
        msg['Subject'] = f"Order Confirmation - {order_data['order_number']}"
        msg['From'] = MAIL_FROM
        msg['To'] = order_data['email']

        items_html = "".join(
            f"<li>{item['name']} - Quantity: {item['qty']}</li>"
            for item in order_data.get('items', [])
        )

        html_body = f"""
        <html>
        <body>
            <h2>Order Confirmation</h2>
            <p>Thank you for your order!</p>
            <p><strong>Order Number:</strong> {order_data['order_number']}</p>
            <p><strong>Order ID:</strong> {order_data['id']}</p>
            <p><strong>Total Amount:</strong> ${order_data['amount']}</p>
            <p><strong>Status:</strong> {order_data['status']}</p>
            <h3>Items:</h3>
            <ul>{items_html}</ul>
        </body>
        </html>
        """

        text_body = f"""
        Order Confirmation
        Order Number: {order_data['order_number']}
        Order ID: {order_data['id']}
        Total Amount: ${order_data['amount']}
        Status: {order_data['status']}
        Items:
        """
        for item in order_data.get('items', []):
            text_body += f"- {item['name']} - Quantity: {item['qty']}\n"

        msg.attach(MIMEText(text_body, 'plain'))
        msg.attach(MIMEText(html_body, 'html'))

        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.starttls()
            server.login(SMTP_USER, SMTP_PASS)
            server.send_message(msg)

        log(f"Email sent to {order_data['email']} for order {order_data['order_number']}",
            level="INFO", obj=order_data)
        return True
    except Exception as e:
        log(f"Failed to send email for order {order_data['order_number']}: {e}",
            level="ERROR", obj=order_data)
        return False

# -------------------- Consumer --------------------
def consume_orders():
    if not ENABLE_ORDER_EMAIL_CONSUMER:
        log("Order email consumer is disabled", level="INFO")
        return

    consumer_config = {
        'bootstrap.servers': BROKER,
        'group.id': 'email-consumer-group',
        'auto.offset.reset': 'earliest',  # start from beginning if no committed offset
        'enable.auto.commit': False,
        'api.version.request': True,
        'socket.timeout.ms': 60000,
        'session.timeout.ms': 10000
    }
    consumer = Consumer(consumer_config)
    consumer.subscribe([TOPIC])

    log(f"Email Consumer started | Broker: {BROKER} | Topic: {TOPIC}", level="INFO")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    log(f"Reached end of partition {msg.partition()}", level="INFO")
                else:
                    log(f"Consumer error: {msg.error()}", level="ERROR")
                continue

            # Parse the order message
            order = json.loads(msg.value().decode('utf-8'))
            log(f"Processing Order ID: {order['id']} | Email: {order['email']} | Partition: {msg.partition()} | Offset: {msg.offset()}",
                level="INFO", obj=order)

            # Send email
            if send_order_email(order):
                consumer.commit(msg)  # mark as processed
                send_status_update(order["id"], "email_sent")
                log(f"Order {order['id']} processed successfully", level="INFO", obj=order)
            else:
                log(f"Failed to process order {order['id']}", level="ERROR", obj=order)
    except KeyboardInterrupt:
        log("Shutting down email consumer...", level="INFO")
    finally:
        consumer.close()
        log("Email consumer closed", level="INFO")

# -------------------- Main --------------------
if __name__ == "__main__":
    consume_orders()
