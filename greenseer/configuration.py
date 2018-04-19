import configparser
import os

default_configuration_file_path = os.path.dirname(__file__) + '/config.properties'


class Config:
    def __init__(self, path=default_configuration_file_path):
        self.__config = None
        self.refresh_config(path)

    def refresh_config(self, path):
        self.__config = configparser.RawConfigParser()
        self.__config.read(path)

    def get_str(self, section, key, default=''):
        return self.__config.get(section, key, fallback=default)

    def get_int_value(self, section, key, default=''):
        return int(self.get_str(section, key, default))


def create_configuration(path=default_configuration_file_path) -> Config:
    return Config(path)


global_configuration = create_configuration()


def do_global_configuration(path=default_configuration_file_path):
    global_configuration.refresh_config(path)


def get_global_configuration() -> Config:
    return global_configuration
