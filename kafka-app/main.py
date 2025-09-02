from config.settings import (
    ENABLE_ORDER_CONSUMER,
    ENABLE_ORDER_DB_CONSUMER,
    ENABLE_ORDER_EMAIL_CONSUMER
)
from consumer.order_consumer import consume_orders
from consumer.db_consumer import consume_db_updates
from consumer.email_consumer import consume_emails
from utils.logger import log

if __name__ == "__main__":
    if ENABLE_ORDER_CONSUMER == "true":
        consume_orders()
    else:
        log("🚫 Order consumer disabled by config.", "WARNING")

    # DB consumer
    if ENABLE_ORDER_DB_CONSUMER == "true":
        consume_db_updates()
    else:
        log("🚫 DB consumer disabled by config.", "WARNING")

    # Email consumer
    if ENABLE_ORDER_EMAIL_CONSUMER == "true":
        consume_emails()
    else:
        log("🚫 Email consumer disabled by config.", "WARNING")
