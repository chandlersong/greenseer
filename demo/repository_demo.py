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

import unittest
from logging.config import fileConfig
from unittest import TestCase

from greenseer.repository.china_stock import get_global_basic_info_repository, \
    create_china_stock_assert_repository, create_china_stock_cash_repository, create_china_stock_income_repository

fileConfig('logging_config.ini')


class TestChinaStock(TestCase):

    def test_load_basic_info(self):
        repository = get_global_basic_info_repository()

        print(repository.load_data("600096"))
        print(get_global_basic_info_repository().load_data("600096"))

    def test_china_assert_repository(self):
        latest_report = '2018-03-31'
        print("asssert_report")
        print(create_china_stock_assert_repository(base_folder="reportData").load_data("600096")[latest_report])
        print("cash report")
        print(create_china_stock_cash_repository(base_folder="reportData").load_data("600096")[latest_report])
        print("income report")
        print(create_china_stock_income_repository(base_folder="reportData").load_data("600096")[latest_report])


if __name__ == '__main__':
    unittest.main()
