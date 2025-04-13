import sys
import logging

logging_config = dict(
    version=1,
    formatters={
        'verbose': {
            'format': ("[%(asctime)s] %(levelname)s "
                       "[%(name)s:%(lineno)s] %(message)s"),
            'datefmt': "%d/%b/%Y %H:%M:%S",
        },
        'simple': {
            'format': '%(levelname)s %(message)s',
        },
    },
    handlers={
        'debug-logger': {'class': 'logging.handlers.RotatingFileHandler',
                         'formatter': 'verbose',
                         'level': logging.DEBUG,
                         'filename': 'logfile.log',
                         'maxBytes': 52428800,
                         'backupCount': 7},

        'console': {
            'class': 'logging.StreamHandler',
            'level': 'DEBUG',
            'formatter': 'simple',
            'stream': sys.stdout,
        },

    },
    loggers={
        'program_logger': {
            'handlers': ['debug-logger', 'console'],
            'level': logging.DEBUG
        },

    }
)


