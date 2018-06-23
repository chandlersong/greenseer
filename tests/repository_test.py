import os
import shutil
import unittest
from datetime import datetime, timedelta
from logging.config import fileConfig
from unittest import TestCase
from unittest.mock import MagicMock, Mock, call, patch

import numpy as np
import pandas as pd
from freezegun import freeze_time
from numpy.matlib import randn
from pandas import DataFrame
from pandas.util.testing import assert_frame_equal

from greenseer.repository import FolderSource, LocalSource, BaseRepository, TimeSeriesRemoteFetcher, FileSource
from greenseer.repository.china_stock import DailyPriceRepository, TuShareHDataFetcher, TuShareStockBasicFetcher, \
    BasicInfoRepository, ChinaAssertRepository
from tests.file_const import DEFAULT_TEST_FOLDER, read_sina_600096_test_data, \
    read_compression_data_to_dataframe, read_sina_600096_test_data_dirty, read_china_total_stock_info, \
    TOTAL_STOCK_INFO_PATH

TEST_STOCK_ID = "600096"

DEFAULT_FOLDER = DEFAULT_TEST_FOLDER + "/repository_test"
fileConfig('logging_config.ini')


class TestFolderSource(TestCase):
    def setUp(self):
        self.stock_id = TEST_STOCK_ID
        self.source = FolderSource(DEFAULT_FOLDER)
        self.expected_path = DEFAULT_FOLDER + '/600096.tar.gz'

    def tearDown(self):
        if os.path.exists(DEFAULT_FOLDER):
            shutil.rmtree(DEFAULT_FOLDER)

    def test_initial_file_source(self):
        expect_folder = self.source.source_folder

        self.assertEqual(DEFAULT_FOLDER, expect_folder)
        # folder should be auto created
        self.assertTrue(os.path.exists(expect_folder))

    def test_refresh_data_exist(self):
        previous_data = DataFrame(randn(5, 2), index=range(0, 10, 2), columns=list('AB'))
        previous_data.to_csv(self.expected_path, compression="gzip")

        data = read_sina_600096_test_data()
        self.source.refresh_data(data, self.stock_id)

        self.assertTrue(os.path.exists(self.expected_path))
        assert_frame_equal(data, read_compression_data_to_dataframe(self.expected_path))

    def test_refresh_data_new(self):
        data = read_sina_600096_test_data()
        self.source.refresh_data(data, self.stock_id)

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

        self.source.append_data(append_data, self.stock_id)

        assert_frame_equal(data.append(append_data),
                           read_compression_data_to_dataframe(self.expected_path))


class TestBaseRepository(TestCase):

    def setUp(self):
        self.mock_remote_source = MagicMock()
        self.mock_remote_source.__name__ = "mock_remote_function"
        self.mock_local_source = MagicMock(spec=LocalSource)

        self.repository = BaseRepository(self.mock_local_source)

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

        self.mock_local_source.append_data.assert_called_once_with(self.__remote_data, TEST_STOCK_ID)
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


class TestTimeSeriesRemoteFetcher(TestCase):

    def setUp(self):
        self.mock_remote_source = MagicMock()
        self.mock_remote_source.__name__ = "mock_remote_function"

        self.fetcher = TimeSeriesRemoteFetcher(self.mock_remote_source, block_sleep_seconds=1)

        self.__remote_data = DataFrame(
            {'open': [1, 2], 'high': [3, 4], 'close': [5, 6], 'low': [7, 8], 'volume': [9, 10], 'amount': [11, 12]},
            index=(pd.Index(pd.date_range('7/1/2017', periods=2), name='date')))
        self.__local_data = read_sina_600096_test_data()
        self.__dirty_data = read_sina_600096_test_data_dirty()

        self.__start_date = datetime.strptime("2017-06-30", DailyPriceRepository.DEFAULT_DATE_FORMAT)
        self.__end_date = datetime.now()

    def test_load_remote_normal(self):
        self.fetcher.do_load_remote = Mock(return_value=self.__remote_data)

        assert_frame_equal(self.__remote_data,
                           self.fetcher.load_remote(TEST_STOCK_ID, self.__start_date, self.__end_date))

    def test_load_remote_io_exception(self):
        self.fetcher.do_load_remote = Mock(side_effect=[IOError, self.__remote_data])

        assert_frame_equal(self.__remote_data,
                           self.fetcher.load_remote(TEST_STOCK_ID, self.__start_date, self.__end_date))

    def test_load_data_by_period_success(self):
        day_gap = 365
        first_day = datetime.now() - timedelta(days=(day_gap + 100))  # make sure remote will be load twice
        self.fetcher.get_stock_first_day = Mock(return_value=first_day)
        self.fetcher.random_fetch_days = Mock(return_value=timedelta(day_gap))
        self.fetcher.load_remote = Mock(side_effect=[self.__local_data, self.__remote_data])
        self.fetcher.random_sleep_seconds = Mock(return_value=0)

        expected = pd.concat([self.__local_data, self.__remote_data]).sort_index()

        actual = self.fetcher.load_data_by_period(TEST_STOCK_ID)

        assert_frame_equal(expected, actual)

    @freeze_time("2018-05-05")
    def test_load_data_by_period_data_calculate(self):
        day_gap = 365
        now = datetime.today()
        first_day = now - timedelta(days=(day_gap + 100))  # make sure remote will be load twice
        self.fetcher.get_stock_first_day = Mock(return_value=first_day)
        self.fetcher.random_fetch_days = Mock(return_value=timedelta(day_gap))
        self.fetcher.load_remote = Mock(side_effect=[self.__local_data, self.__remote_data])
        self.fetcher.random_sleep_seconds = Mock(return_value=0)

        self.fetcher.load_data_by_period(TEST_STOCK_ID)

        first_period_end = first_day + timedelta(day_gap)
        calls = [
            call(TEST_STOCK_ID, first_day, first_period_end),
            call(TEST_STOCK_ID, first_period_end + timedelta(days=1), now),
        ]
        self.fetcher.load_remote.assert_has_calls(calls, any_order=True)


class TestFileSource(TestCase):

    def setUp(self):
        self.__test_data = read_china_total_stock_info()
        self.__source_path = DEFAULT_TEST_FOLDER + "/test_file_source.gz"
        self.__source = FileSource(self.__source_path)

    def tearDown(self):
        if os.path.exists(self.__source_path):
            os.remove(self.__source_path)

    def read_china_total_stock_info_test(self):
        """
        it's a integration test. need to connect to internet
        :return:
        """
        import tushare as ts
        result = ts.get_stock_basics()
        result.to_csv(TOTAL_STOCK_INFO_PATH, compression="gzip")

        assert_frame_equal(result, read_china_total_stock_info())

    def test_initial_repository(self):
        source = FileSource(TOTAL_STOCK_INFO_PATH)

        assert_frame_equal(read_china_total_stock_info(), source.cache)

        self.assertTrue(source.cache_enabled)

    def test_initial_repository_format_not_correct(self):
        with self.assertRaises(OSError):
            FileSource("__init__.py")

    def test_initial_repository_empty(self):
        source = FileSource(self.__source_path)
        self.assertFalse(os.path.exists(self.__source_path))
        self.assertFalse(source.cache_enabled)

    def test_append_data(self):
        self.__source.refresh_data = Mock()

        data = read_china_total_stock_info()
        self.__source.append_data(data)

        self.__source.refresh_data.assert_called_once_with(data)

    def test_refresh_data(self):
        self.__source.refresh_data(self.__test_data)

        assert_frame_equal(self.__test_data, self.__source.cache)

        actual = pd.read_csv(TOTAL_STOCK_INFO_PATH, dtype={"code": np.str}, compression="gzip").set_index(
            'code').sort_index()
        assert_frame_equal(self.__test_data, actual)

    def test_refresh_data_exist(self):
        DataFrame(randn(5, 2), index=range(0, 10, 2), columns=list('AB')).to_csv(self.__source_path, compression="gzip")

        self.__source.refresh_data(self.__test_data)

        assert_frame_equal(self.__test_data, self.__source.cache)

        actual = pd.read_csv(TOTAL_STOCK_INFO_PATH, dtype={"code": np.str}, compression="gzip").set_index(
            'code').sort_index()
        assert_frame_equal(self.__test_data, actual)

    def test_load_data(self):
        stock_id = "600000"

        data = read_china_total_stock_info()
        expect = data.loc[[stock_id]]

        self.__source.refresh_data(self.__test_data)

        assert_frame_equal(expect, self.__source.load_data(stock_id))

    def test_load_data_not_exist(self):
        self.assertTrue(self.__source.load_data("600000").empty)


@patch("tushare.get_stock_basics")
class TestTuShareStockBasicFetcher(TestCase):

    def setUp(self):
        self.__fetcher = TuShareStockBasicFetcher()
        self.__data = read_china_total_stock_info()
        self.__dirty_data = DataFrame(
            {'open': [1, 2], 'high': [3, 4], 'close': [5, 6], 'low': [7, 8], 'volume': [9, 10], 'amount': [11, 12]},
            index=(pd.Index(pd.date_range('7/1/2017', periods=2), name='date')))
        self.__stock_id = "600000"

    def test_initial_remote_data_refresh_cache(self, remote_method):
        self.assertIsNone(self.__fetcher.cache)
        remote_method.return_value = self.__data.copy()

        self.__fetcher.initial_remote_data()

        assert_frame_equal(self.__data, self.__fetcher.cache)

    def test_load_remote_cache_exists(self, remote_method):
        remote_method.return_value = self.__data.copy()
        self.__fetcher.initial_remote_data()

        actual = self.__fetcher.load_remote(self.__stock_id)
        assert_frame_equal(self.__data.loc[[self.__stock_id]], actual)

    def test_load_remote_cache_not_exists(self, remote_method):
        remote_method.return_value = self.__data.copy()
        self.assertIsNone(self.__fetcher.cache)

        actual = self.__fetcher.load_remote(self.__stock_id)
        assert_frame_equal(self.__data.loc[[self.__stock_id]], actual)


class TestChinaAssertRepository(TestCase):

    def setUp(self):
        self.mock_local_source = MagicMock(spec=LocalSource)
        self.__repository = ChinaAssertRepository(self.mock_local_source)





class TestBaseInfoRepository(TestCase):

    def setUp(self):
        self.__repository = BasicInfoRepository()
        self.__data = read_china_total_stock_info()
        self.__stock_id = "600000"

    def test_load_data(self):
        self.__repository.load_remote = Mock(return_value=self.__data.loc[[self.__stock_id]])

        actual = self.__repository.load_data(self.__stock_id)
        assert_frame_equal(self.__data.loc[[self.__stock_id]], actual)


if __name__ == '__main__':
    unittest.main()
