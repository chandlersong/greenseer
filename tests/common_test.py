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


import unittest
from unittest import TestCase

from greenseer.configuration import create_configuration


class TestConfiguration(TestCase):

    def test_auto_load(self):
        config = create_configuration()

        self.assertEqual('365', config.get_str("china_stock_config", "remote_fetch_days"))
        self.assertEqual(365, config.get_int_value("china_stock_config", "remote_fetch_days"))

    def test_manually_load(self):
        config = create_configuration("config/test_config.properties")

        self.assertEqual('366', config.get_str("china_stock_config", "remote_fetch_days"))
        self.assertEqual(366, config.get_int_value("china_stock_config", "remote_fetch_days"))


if __name__ == "__main__":
    if __name__ == '__main__':
        unittest.main()
