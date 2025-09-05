import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
# Look for .env file in the same directory as this settings.py file's parent
env_path = Path(__file__).parent.parent / '.env'
load_dotenv(env_path)

# Kafka settings
BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092,localhost:9093,localhost:9094')
TOPIC = os.getenv('TOPIC_NAME', 'orders')
CONSUMER_GROUP = os.getenv('CONSUMER_GROUP', 'email-consumer')

# Email settings
SMTP_HOST = os.getenv('SMTP_HOST', 'smtp.gmail.com')
SMTP_PORT = int(os.getenv('SMTP_PORT', '587'))
SMTP_USER = os.getenv('SMTP_USER', '')
SMTP_PASS = os.getenv('SMTP_PASS', '')
MAIL_FROM = os.getenv('MAIL_FROM', 'noreply@example.com')
ENABLE_ORDER_EMAIL_CONSUMER = os.getenv('ENABLE_ORDER_EMAIL_CONSUMER', 'true').lower() == 'true'

# Database settings (if needed)
DB_HOST = os.getenv('DB_HOST', 'postgres')
DB_PORT = int(os.getenv('DB_PORT', '5432'))
DB_NAME = os.getenv('DB_DATABASE', 'orders_db')
DB_USER = os.getenv('DB_USERNAME', 'postgres')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'password')


