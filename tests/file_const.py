import tempfile
from unittest.mock import MagicMock

import numpy as np
import pandas as pd
from pandas import DataFrame

DEFAULT_TEST_FOLDER = tempfile.gettempdir() + "/greenseer_test"

SINA_600096_2017_6_21_TO_6_30 = "data/sina_600096_2017_6_21_to_6_30.gzip"
SINA_600096_2017_6_21_TO_6_30_DIRTY = "data/sina_600096_2017_6_21_to_6_30_dirty.csv"

TOTAL_STOCK_INFO_PATH = "data/china_tushare_total_stock.gzip"

NETEASE_600096_ASSERT_REPORT = "data/600096_assert_report.gz"


def read_sina_600096_test_data() -> DataFrame:
    return read_compression_data_to_dataframe(SINA_600096_2017_6_21_TO_6_30).sort_index()


def read_sina_600096_test_data_dirty() -> DataFrame:
    return read_data_to_dataframe(SINA_600096_2017_6_21_TO_6_30_DIRTY).sort_index()


def read_china_total_stock_info() -> DataFrame:
    actual_flat = pd.read_csv(TOTAL_STOCK_INFO_PATH, dtype={"code": np.str}, compression="gzip")
    return actual_flat.set_index('code').sort_index()


def read_data_to_dataframe(file_path_or_buf, compression='infer') -> DataFrame:
    return pd.read_csv(file_path_or_buf, index_col=0, compression=compression, parse_dates=True).sort_index()


def read_compression_data_to_dataframe(file_path_or_buf) -> DataFrame:
    return read_data_to_dataframe(file_path_or_buf, compression="gzip").sort_index()


def read_600096_assert_reports() -> DataFrame:
    return read_data_to_dataframe(NETEASE_600096_ASSERT_REPORT)


def create_mock_request_urlopen(self, context="mock call"):
    cm = MagicMock()
    cm.getcode.return_value = 200
    cm.read.return_value = context
    cm.__enter__.return_value = cm
    return cm
