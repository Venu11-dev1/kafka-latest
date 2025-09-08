import logging
import json
import sys
from pathlib import Path

# Use Docker-mounted logs directory
LOG_FILE_PATH = "/app/logs/kafka_app.log"
Path("/app/logs").mkdir(parents=True, exist_ok=True)

class JsonFormatter(logging.Formatter):
    def format(self, record):
        log_record = {
            "time": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "message": record.getMessage(),
        }
        if hasattr(record, 'obj') and record.obj is not None:
            log_record['obj'] = record.obj
        return json.dumps(log_record)

# ---------- Stream Handler (stdout) ----------
stdout_handler = logging.StreamHandler(sys.stdout)
stdout_handler.setFormatter(JsonFormatter())

# ---------- File Handler ----------
file_handler = logging.FileHandler(LOG_FILE_PATH)
file_handler.setFormatter(JsonFormatter())

# ---------- Logger ----------
logger = logging.getLogger("kafka_app")
logger.setLevel(logging.INFO)
logger.addHandler(stdout_handler)
logger.addHandler(file_handler)

def log(message: str, level: str = "INFO", obj: dict = None):
    if obj:
        logger.log(getattr(logging, level.upper(), logging.INFO), message, extra={"obj": obj})
    else:
        logger.log(getattr(logging, level.upper(), logging.INFO), message)
