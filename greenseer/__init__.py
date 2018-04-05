import configparser
import logging
import os

default_configuration_file_path = os.path.dirname(__file__) + '/config.properties'


class Config:
    def __init__(self, path=default_configuration_file_path):
        self.config = configparser.RawConfigParser()
        self.config.read(path)

    def get(self, section, key, default=''):
        return self.config.get(section, key, fallback=default)


def create_logger(name) -> logging.Logger:
    return logging.getLogger(name)


h_config = Config()
