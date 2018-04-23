import os
import shutil
import unittest
from datetime import datetime
from logging.config import fileConfig
from unittest import TestCase
from unittest.mock import MagicMock, Mock

import pandas as pd
from numpy.matlib import randn
from pandas import DataFrame
from pandas.util.testing import assert_frame_equal

from greenseer.repository import FolderSource, LocalSource
from greenseer.repository.china_stock import DailyPriceRepository, TuShareHDataFetcher
from tests.file_const import DEFAULT_TEST_FOLDER, read_sina_600096_test_data, \
    read_compression_data_to_dataframe, read_sina_600096_test_data_dirty

TEST_STOCK_ID = "600096"

DEFAULT_FOLDER = DEFAULT_TEST_FOLDER + "/repository_test"
fileConfig('logging_config.ini')


class TestFolderSource(TestCase):
    def setUp(self):
        if os.path.exists(DEFAULT_FOLDER):
            shutil.rmtree(DEFAULT_FOLDER)

        self.stock_id = TEST_STOCK_ID
        self.source = FolderSource(DEFAULT_FOLDER)
        self.expected_path = DEFAULT_FOLDER + '/600096.tar.gz'

    def test_initial_file_source(self):
        expect_folder = self.source.source_folder

        self.assertEqual(DEFAULT_FOLDER, expect_folder)
        # folder should be auto created
        self.assertTrue(os.path.exists(expect_folder))

    def test_initial_data(self):
        data = read_sina_600096_test_data()

        self.source.initial_data(self.stock_id, data)

        self.assertTrue(os.path.exists(self.expected_path))
        assert_frame_equal(data, read_compression_data_to_dataframe(self.expected_path))

    def test_refresh_data_exist(self):
        previous_data = DataFrame(randn(5, 2), index=range(0, 10, 2), columns=list('AB'))
        previous_data.to_csv(self.expected_path, compression="gzip")

        data = read_sina_600096_test_data()
        self.source.refresh_data(self.stock_id, data)

        self.assertTrue(os.path.exists(self.expected_path))
        assert_frame_equal(data, read_compression_data_to_dataframe(self.expected_path))

    def test_refresh_data_new(self):
        data = read_sina_600096_test_data()
        self.source.refresh_data(self.stock_id, data)

        self.assertTrue(os.path.exists(self.expected_path))
        assert_frame_equal(data, read_compression_data_to_dataframe(self.expected_path))

    def test_load_data(self):
        data = read_sina_600096_test_data()
        data.to_csv(self.expected_path, compression="gzip")

        expected_data = self.source.load_data(self.stock_id)
        assert_frame_equal(data, expected_data)

    def test_load_data_empty(self):
        expected_data = self.source.load_data(self.stock_id)

        self.assertTrue(expected_data.empty)

    def test_append_data(self):
        append_data = pd.DataFrame({
            'open': [1, 2],
            'high': [3, 4],
            'close': [5, 6],
            'low': [7, 8],
            'volume': [9, 10],
            'amount': [11, 12]
        }, index=(pd.Index(pd.date_range('7/1/2017', periods=2), name='date')))

        data = read_sina_600096_test_data()
        data.to_csv(self.expected_path, compression='gzip')

        self.source.append_data(self.stock_id, append_data)

        assert_frame_equal(data.append(append_data),
                           read_compression_data_to_dataframe(self.expected_path))


class TestChinaStockModuleDailyPriceRepository(TestCase):

    def setUp(self):
        self.mock_remote_source = MagicMock()
        self.mock_remote_source.__name__ = "mock_remote_function"
        self.mock_local_source = MagicMock(spec=LocalSource)

        self.repository = DailyPriceRepository(self.mock_local_source, self.mock_remote_source)

        self.__remote_data = DataFrame(
            {'open': [1, 2], 'high': [3, 4], 'close': [5, 6], 'low': [7, 8], 'volume': [9, 10], 'amount': [11, 12]},
            index=(pd.Index(pd.date_range('7/1/2017', periods=2), name='date')))
        self.__local_data = read_sina_600096_test_data()
        self.__dirty_data = read_sina_600096_test_data_dirty()
        self.repository.initial_remote_data = MagicMock(return_value=self.__remote_data)

    def test_load_data_auto_initial_when_local_is_empty(self):
        self.data_from_local(False)

        assert_frame_equal(self.__remote_data, self.repository.load_data(TEST_STOCK_ID))

        self.repository.save_or_update_local.assert_called_once_with(TEST_STOCK_ID, self.__remote_data)

    def test_load_data_form_local_when_local_is_not_empty(self):
        self.data_from_local(True)

        assert_frame_equal(self.__local_data,
                           self.repository.load_data(TEST_STOCK_ID))

    def test_load_and_update_local(self):
        self.data_from_local(True)

        self.repository.load_data(TEST_STOCK_ID)

        self.repository.append_local_if_necessary.assert_called_once_with(TEST_STOCK_ID, self.__local_data)

    def test_load_data_form_remote_when_local_is_dirty(self):
        self.data_from_local(False, dirty_data=True, local_data=self.__local_data)

        assert_frame_equal(self.__remote_data,
                           self.repository.load_data(TEST_STOCK_ID))
        self.repository.save_or_update_local.assert_called_once_with(TEST_STOCK_ID, self.__remote_data)

    def data_from_local(self, from_local, dirty_data=False, local_data=None):
        if local_data is None and from_local is True:
            local_data = self.__local_data
        elif local_data is None and from_local is False:
            local_data = DataFrame()

        self.mock_local_source.load_data = Mock(return_value=local_data)
        self.repository.check_data_dirty = Mock(return_value=dirty_data)
        if from_local:
            self.repository.append_local_if_necessary = Mock()
            '''
            because local will be sort at load_data and there's some bug can't compare two same dataframe
            use the mock here
            '''
            self.__local_data.sort_index = Mock(return_value=self.__local_data)
        else:
            self.repository.save_or_update_local = Mock()

    def test_find_update_date_return_null_if_no_need(self):
        today = datetime.strptime("2017-06-30", DailyPriceRepository.DEFAULT_DATE_FORMAT)
        self.assertIsNone(self.repository.find_update_date(self.__local_data, today))

    def test_find_update_date_after_last_day(self):
        today = datetime.strptime("2017-07-28", DailyPriceRepository.DEFAULT_DATE_FORMAT)
        self.assertEqual(datetime.strptime("2017-07-01", DailyPriceRepository.DEFAULT_DATE_FORMAT),
                         self.repository.find_update_date(self.__local_data, today))

    def test_append_local_if_necessary_not_append(self):
        """
        if update date is none, don't need to do anything
        :return:
        """
        self.repository.find_update_date = Mock(return_value=None)
        self.mock_local_source.append_data = Mock()

        self.repository.append_local_if_necessary(TEST_STOCK_ID, self.__local_data)

        self.assertEqual(0, self.mock_local_source.append_data.call_count)

    def test_append_local_if_necessary_append(self):
        """
        if update date is none, don't need to do anything
        :return:
        """
        next_day = "2017-07-01"
        self.repository.find_update_date = Mock(
            return_value=datetime.strptime(next_day, DailyPriceRepository.DEFAULT_DATE_FORMAT))
        self.mock_remote_source.return_value = self.__remote_data
        self.mock_local_source.append_data = Mock()

        self.repository.append_local_if_necessary(TEST_STOCK_ID, self.__local_data)

        self.mock_local_source.append_data.assert_called_once_with(TEST_STOCK_ID, self.__remote_data)
        self.mock_remote_source.assert_called_once_with(TEST_STOCK_ID, start=next_day,
                                                        end=datetime.now().strftime(
                                                            DailyPriceRepository.DEFAULT_DATE_FORMAT), autype='hfq')


class TestTuShareHDataFetcher(TestCase):

    def setUp(self):
        self.__mock_remote_source = Mock()
        self.__mock_remote_source.__name__ = "mock_remote_source"
        self.fetcher = TuShareHDataFetcher(remote_source=self.__mock_remote_source)

        self.__test_data = read_sina_600096_test_data()
        self.__dirty_test_data = read_sina_600096_test_data_dirty()

    def test_check_data_dirty_is_clean(self):
        self.__mock_remote_source.return_value = self.__test_data.tail(1)

        self.assertFalse(self.fetcher.check_data_dirty(TEST_STOCK_ID, self.__test_data))

    def test_check_data_dirty_is_dirty(self):
        self.__mock_remote_source.return_value = self.__dirty_test_data.tail(1)

        self.assertTrue(self.fetcher.check_data_dirty(TEST_STOCK_ID, self.__test_data))


if __name__ == '__main__':
    unittest.main()
