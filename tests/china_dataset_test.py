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
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal

from greenseer.dataset.china_dataset import load_by_stock_id, load_multi_data, compose_target


def create_mock_china_repository():
    result = MagicMock()
    result.assert_report = MagicMock()
    result.income_report = MagicMock()
    result.cash_report = MagicMock()
    return result


def create_expected_data(repository):
    assert_report = pd.DataFrame(np.random.random((3, 3)), index=range(0, 3))
    cash_report = pd.DataFrame(np.random.random((3, 3)), index=range(0, 3))
    income_report = pd.DataFrame(np.random.random((3, 3)), index=range(0, 3))
    repository.assert_report.load_data = MagicMock(return_value=assert_report)
    repository.income_report.load_data = MagicMock(return_value=income_report)
    repository.cash_report.load_data = MagicMock(return_value=cash_report)
    return [assert_report, income_report, cash_report]


class TestLoadByStockId(TestCase):

    def test_load_by_stock_id(self):
        repository = create_mock_china_repository()
        reports = create_expected_data(repository)

        stock_id = "abc"
        expected = pd.concat({stock_id: pd.concat(reports, axis=1)})
        assert_frame_equal(expected, load_by_stock_id(stock_id, False, repository))

    @patch("greenseer.dataset.china_dataset.load_by_stock_id")
    def test_load_multi_data(self, load_stock):
        repository = create_mock_china_repository()
        stock_id_a = "a"
        reports_a = pd.concat(create_expected_data(repository))
        stock_id_b = "b"
        reports_b = pd.concat(create_expected_data(repository))
        load_stock.side_effect = [pd.concat({stock_id_a: reports_a}), pd.concat({stock_id_b: reports_b})]

        expected = pd.concat({stock_id_a: reports_a, stock_id_b: reports_b})
        assert_frame_equal(expected, load_multi_data([stock_id_a, stock_id_b]))

    def test_compose_target_set(self):
        repository = create_mock_china_repository()
        repository.stock_info = pd.DataFrame(np.random.random((3, 3)), index=["a", "b", "c"])

        expect = pd.DataFrame({
            "x": [0, 1, 0],
            "y": [1, 0, 0]
        }, index=["a", "b", "c"])
        actual = compose_target({"x": ["b"], "y": pd.Series("a")}, repository)
        assert_frame_equal(expect, actual)


if __name__ == '__main__':
    unittest.main()
