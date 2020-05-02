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

import os
import shutil
import unittest
from logging.config import fileConfig
from unittest import TestCase
from unittest.mock import MagicMock, Mock, patch

import pandas as pd
import tushare
from pandas import DataFrame
from pandas.util.testing import assert_frame_equal

from greenseer.repository import ReportLocalData, LocalSource
from greenseer.repository.china_stock import TuShareStockBasicFetcher, NetEaseRemoteFetcher
from tests.file_const import DEFAULT_TEST_FOLDER, read_sina_600096_test_data, \
    read_china_total_stock_info, \
    create_mock_request_urlopen, read_600096_assert_reports

TEST_STOCK_ID = "600096"

DEFAULT_FOLDER = DEFAULT_TEST_FOLDER + "/repository_test"
fileConfig('logging_config.ini')


class TestFolderSource(TestCase):
    def setUp(self):
        self.stock_id = TEST_STOCK_ID
        self.source = ReportLocalData(DEFAULT_FOLDER)
        self.expected_path = DEFAULT_FOLDER + '/600096.gz'

    def tearDown(self):
        if os.path.exists(DEFAULT_FOLDER):
            shutil.rmtree(DEFAULT_FOLDER)

    def test_initial_file_source(self):
        expect_folder = self.source.source_folder

        self.assertEqual(DEFAULT_FOLDER, expect_folder)
        # folder should be auto created
        self.assertTrue(os.path.exists(expect_folder))

    def test_load_data(self):
        data = read_sina_600096_test_data()
        data.to_csv(self.expected_path, compression="gzip")

        expected_data = self.source.load_data(self.stock_id)
        assert_frame_equal(data, expected_data)

    def test_load_data_empty(self):
        expected_data = self.source.load_data(self.stock_id)

        self.assertTrue(expected_data.empty)


@patch("tushare.get_stock_basics")
class TestTuShareStockBasicFetcher(TestCase):

    def setUp(self):
        self.__fetcher = TuShareStockBasicFetcher(tushare.get_stock_basics)
        self.__data = read_china_total_stock_info()
        self.__dirty_data = DataFrame(
            {'open': [1, 2], 'high': [3, 4], 'close': [5, 6], 'low': [7, 8], 'volume': [9, 10], 'amount': [11, 12]},
            index=(pd.Index(pd.date_range('7/1/2017', periods=2), name='date')))
        self.__stock_id = "600000"

    def test_load_remote_cache_exists(self, remote_method):
        self.__fetcher.initial_remote_data = Mock(return_value=self.__data.copy())

        actual = self.__fetcher.load_remote(self.__stock_id)
        assert_frame_equal(self.__data.loc[[self.__stock_id]], actual)

    def test_load_remote_cache_not_exists(self, remote_method):
        remote_method.return_value = self.__data.copy()
        self.assertIsNone(self.__fetcher.cache)

        self.__fetcher.initial_remote_data = Mock(return_value=self.__data.copy())
        actual = self.__fetcher.load_remote(self.__stock_id)
        assert_frame_equal(self.__data.loc[[self.__stock_id]], actual)


class NetEaseRemoteFetcherTest(TestCase):
    def setUp(self):
        self.mock_local_source = MagicMock(spec=LocalSource)
        self.__repository = NetEaseRemoteFetcher("mock_path")
        self.__original_data = pd.read_csv("data/600096.origin.csv", na_values='--',
                                           index_col=0)

    @patch("pandas.read_csv")
    @patch("urllib.request.urlopen")
    def test_initial_remote_data(self, request_urlopen, pandas_read_csv):
        request_urlopen.return_value = create_mock_request_urlopen("data/600096.origin.csv")
        pandas_read_csv.return_value = self.__original_data
        assert_frame_equal(read_600096_assert_reports(), self.__repository.initial_remote_data("600096"))


if __name__ == '__main__':
    unittest.main()
