from datetime import datetime
import config


def set_timestamp():
    now = datetime.now()
    config.TIMESTAMP = now.strftime("%Y%m%d-%H%M")
