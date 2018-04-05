import os
import shutil
import unittest
from logging.config import fileConfig
from unittest import TestCase

import pandas as pd
from numpy.matlib import randn
from pandas import DataFrame
from pandas.util.testing import assert_frame_equal

from greenseer.repository import FolderSource
from tests.file_const import DEFAULT_TEST_FOLDER, SINA_600096_2017_6_21_TO_6_30

DEFAULT_FOLDER = DEFAULT_TEST_FOLDER + "/repository_test"
fileConfig('logging_config.ini')


class TestFolderSource(TestCase):
    def setUp(self):
        if os.path.exists(DEFAULT_FOLDER):
            shutil.rmtree(DEFAULT_FOLDER)

        self.stock_id = "600096"
        self.source = FolderSource(DEFAULT_FOLDER)
        self.expected_path = DEFAULT_FOLDER + '/600096.tar.gz'

    def test_initial_file_source(self):
        expect_folder = self.source.source_folder

        self.assertEqual(DEFAULT_FOLDER, expect_folder)
        # folder should be auto created
        self.assertTrue(os.path.exists(expect_folder))

    def test_initial_data(self):
        data = self.load_test_data()

        self.source.initial_data(self.stock_id, data)

        self.assertTrue(os.path.exists(self.expected_path))
        assert_frame_equal(data, pd.read_csv(self.expected_path, index_col=0, compression="gzip", parse_dates=True))

    def test_refresh_data_exist(self):
        previous_data = DataFrame(randn(5, 2), index=range(0, 10, 2), columns=list('AB'));
        previous_data.to_csv(self.expected_path, compression="gzip")

        data = self.load_test_data()
        self.source.refresh_data(self.stock_id, data)

        self.assertTrue(os.path.exists(self.expected_path))
        assert_frame_equal(data, pd.read_csv(self.expected_path, index_col=0, compression="gzip", parse_dates=True))

    def test_refresh_data_new(self):
        data = self.load_test_data()
        self.source.refresh_data(self.stock_id, data)

        self.assertTrue(os.path.exists(self.expected_path))
        assert_frame_equal(data, pd.read_csv(self.expected_path, index_col=0, compression="gzip", parse_dates=True))

    def test_load_data(self):
        data = self.load_test_data()
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

        data = self.load_test_data()
        data.to_csv(self.expected_path, compression='gzip')

        self.source.append_data(self.stock_id, append_data)

        assert_frame_equal(data.append(append_data),
                           pd.read_csv(self.expected_path, index_col=0, compression="gzip", parse_dates=True))

    @staticmethod
    def load_test_data():
        return pd.read_csv(SINA_600096_2017_6_21_TO_6_30, index_col=0, compression="gzip", parse_dates=True)


if __name__ == '__main__':
    unittest.main()
