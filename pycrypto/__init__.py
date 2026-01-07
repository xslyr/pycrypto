from logging.config import dictConfig

__version__ = "0.1.0"

logging_config = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "standard": {"format": "%(asctime)s [%(levelname)s] %(name)s: %(message)s"},
        "detailed": {"format": "%(asctime)s [%(levelname)s] %(name)s:%(lineno)d: %(message)s"},
    },
    "handlers": {
        "console_debug": {
            "level": "DEBUG",
            "formatter": "standard",
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stdout",
        },
        "console_info": {
            "level": "INFO",
            "formatter": "standard",
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stdout",
        },
        "log_all": {
            "level": "ERROR",
            "filename": "logs/all.log",
            "maxBytes": 5242880,
            "backupCount": 5,
            "formatter": "standard",
            "class": "logging.handlers.RotatingFileHandler",
        },
        "log_spot": {
            "level": "WARNING",
            "filename": "logs/spot.log",
            "maxBytes": 5242880,
            "backupCount": 5,
            "formatter": "detailed",
            "class": "logging.handlers.RotatingFileHandler",
        },
        "log_websocket": {
            "level": "WARNING",
            "filename": "logs/websocket.log",
            "maxBytes": 5242880,
            "backupCount": 5,
            "formatter": "detailed",
            "class": "logging.handlers.RotatingFileHandler",
        },
    },
    "loggers": {
        "": {"handlers": ["console_info"], "level": "INFO", "propagate": False},
        "app": {"handlers": ["log_all"], "level": "INFO", "propagate": True},
        "app.spot": {"handlers": ["log_spot"], "level": "INFO", "propagate": True},
        "app.websocket": {
            "handlers": ["log_websocket"],
            "level": "INFO",
            "propagate": True,
        },
    },
}

dictConfig(logging_config)
