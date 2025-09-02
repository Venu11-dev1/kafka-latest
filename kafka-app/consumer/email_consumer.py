from confluent_kafka import Consumer
import json, smtplib
from email.mime.text import MIMEText
from utils.logger import log
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

if ENABLE_ORDER_EMAIL_CONSUMER == "false":
    log("🚫 Email consumer disabled by config.", "WARNING")
    exit(0)


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

        log(f"📧 Email sent to {to_email} for order {order_number}")
    except Exception as e:
        log(f"❌ Email failed: {e}", "ERROR")

def consume_emails():
    consumer = Consumer({
        'bootstrap.servers': BROKER,
        'group.id': 'orders-email-group',
        'auto.offset.reset': 'earliest'
    })
    consumer.subscribe([TOPIC])
    log(f"📥 Email Consumer listening on '{TOPIC}'...")
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            elif msg.error():
                log(f"Kafka error: {msg.error()}", "ERROR")
            else:
                order = json.loads(msg.value().decode('utf-8'))
                send_email(order['email'], order['order_number'])
    except KeyboardInterrupt:
        log("Stopping email consumer...")
    finally:
        consumer.close()

if __name__ == "__main__":
    consume_emails()
