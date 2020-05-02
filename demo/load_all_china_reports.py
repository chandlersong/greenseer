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
from unittest import TestCase

from greenseer.dataset.china_dataset import fetch_multi_report, set_local_path
from greenseer.repository.china_stock import get_global_basic_info_repository


class TestDataSet(TestCase):

    def test_load_all_stock(self):
        set_local_path("allReportsData")
        repository = get_global_basic_info_repository()
        info = repository.load_data()
        print(info.index)
        fetch_multi_report(info.index, max_sleep_seconds=5)


if __name__ == '__main__':
    unittest.main()
