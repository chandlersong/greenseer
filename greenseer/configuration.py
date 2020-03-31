#  Copyright (c) 2020 RumorMill (https://chandlersong.me)
#  Copyright (c) 2020 chandler.song
#
#  Licensed under the GNU GENERAL PUBLIC LICENSE v3.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       https://www.gnu.org/licenses/gpl-3.0.html
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

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
