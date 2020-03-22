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
from unittest.mock import MagicMock

import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal

from greenseer.dataset.china_dataset import load_by_stock_Id, ASSERT_REPORT, CASH_REPORT, INCOME_REPORT


def create_mock_china_repository():
    result = MagicMock()
    result.assertReport = MagicMock()
    result.incomeReport = MagicMock()
    result.cashReport = MagicMock()
    return result


def create_expected_data():
    repository = create_mock_china_repository()
    assert_report = pd.DataFrame(np.random.random((3, 3)), index=range(0, 3))
    cash_report = pd.DataFrame(np.random.random((3, 3)), index=range(0, 3))
    income_report = pd.DataFrame(np.random.random((3, 3)), index=range(0, 3))
    repository.assertReport.load_data = MagicMock(return_value=assert_report)
    repository.incomeReport.load_data = MagicMock(return_value=income_report)
    repository.cashReport.load_data = MagicMock(return_value=cash_report)
    return repository, {ASSERT_REPORT: assert_report, INCOME_REPORT: income_report, CASH_REPORT: cash_report}


class TestLoadByStockId(TestCase):

    def test_load_by_stock_id(self):
        repository, reports = create_expected_data()

        expected = pd.concat(reports)
        assert_frame_equal(expected, load_by_stock_Id("abc", False, repository))


if __name__ == '__main__':
    unittest.main()
