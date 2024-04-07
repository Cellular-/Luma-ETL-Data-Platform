import logging.config
from configparser import ConfigParser

CONFIG_FILE = './config/app.config'
LOG_CONFIG_FILE = './config/logging.conf'

def create_config(config_file=None):
    parser = ConfigParser()
    parser.read(config_file or CONFIG_FILE)
    return parser

def get_config():
    return CONFIG

def setup_logging():
    logging.config.fileConfig(LOG_CONFIG_FILE)

CONFIG = create_config()
