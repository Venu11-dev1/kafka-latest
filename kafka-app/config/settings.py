import os
from dotenv import load_dotenv

load_dotenv()

BROKER = os.getenv("KAFKA_BROKER")
TOPIC = os.getenv("TEST_TOPIC")

SMTP_HOST = os.getenv("SMTP_HOST")
SMTP_PORT = int(os.getenv("SMTP_PORT", 587))
SMTP_USER = os.getenv("SMTP_USER")
SMTP_PASS = os.getenv("SMTP_PASS")
MAIL_FROM = os.getenv("MAIL_FROM")

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_DATABASE")
DB_USER = os.getenv("DB_USERNAME")
DB_PASS = os.getenv("DB_PASSWORD")

ENABLE_ORDER_CONSUMER = os.getenv("ENABLE_ORDER_CONSUMER", "true").lower()

ENABLE_ORDER_EMAIL_CONSUMER = os.getenv("ENABLE_ORDER_EMAIL_CONSUMER", "true").lower()
ENABLE_ORDER_DB_CONSUMER = os.getenv("ENABLE_ORDER_DB_CONSUMER", "true").lower()


