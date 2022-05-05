import logging
import sys
from logging.handlers import TimedRotatingFileHandler
from EnvVar import EnvVar

FORMATTER = logging.Formatter("%(asctime)s %(name)-12s %(levelname-8s %(message)s)")
LOG_FILE = EnvVar.project_path+"logs/application.log"

def get_console_handler():
    console_handler=logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(FORMATTER)
    return console_handler

def get_file_handler():
    file_handler = TimedRotatingFileHandler(LOG_FILE,when='midnight')
    file_handler.suffix = '%Y%m%d%H%M%S'
    file_handler.setFormatter(FORMATTER)
    return file_handler

def get_logger(logger_name):
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.info())
    logger.addHandler(get_console_handler())
    logger.addHandler(get_file_handler())
    logger.propagate = False
    return logger

class LogUtil:
    logging.basicConfig(filename="/home/satishmekkonda/CoBaGcpPoc/logs/execution.log",
                        format='%(asctime)s %(message)s',
                        filemode='a')
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
