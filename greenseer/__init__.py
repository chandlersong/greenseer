#  Copyright (c) 2020 GreenSeer (https://chandlersong.me)
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
