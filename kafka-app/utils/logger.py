# utils/logger.py
import logging, json, sys

class JsonFormatter(logging.Formatter):
    def format(self, record):
        log_record = {
            "time": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "message": record.getMessage(),
        }
        # include extra object if provided
        if hasattr(record, 'obj') and record.obj is not None:
            log_record['obj'] = record.obj
        return json.dumps(log_record)

handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(JsonFormatter())
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(handler)

def log(message: str, level: str = "INFO", obj: dict = None):
    if obj:
        logger.log(getattr(logging, level.upper(), logging.INFO), message, extra={"obj": obj})
    else:
        logger.log(getattr(logging, level.upper(), logging.INFO), message)
