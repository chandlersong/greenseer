import tempfile

import pandas as pd
from pandas import DataFrame

DEFAULT_TEST_FOLDER = tempfile.gettempdir() + "/greenseer_test"

SINA_600096_2017_6_21_TO_6_30 = "data/sina_600096_2017_6_21_to_6_30.gzip"
SINA_600096_2017_6_21_TO_6_30_DIRTY = "data/sina_600096_2017_6_21_to_6_30_dirty.csv"


def read_sina_600096_test_data() -> DataFrame:
    return read_compression_data_to_dataframe(SINA_600096_2017_6_21_TO_6_30).sort_index()


def read_sina_600096_test_data_dirty() -> DataFrame:
    return read_data_to_dataframe(SINA_600096_2017_6_21_TO_6_30_DIRTY).sort_index()


def read_data_to_dataframe(file_path_or_buf, compression='infer') -> DataFrame:
    return pd.read_csv(file_path_or_buf, index_col=0, compression=compression, parse_dates=True).sort_index()


def read_compression_data_to_dataframe(file_path_or_buf) -> DataFrame:
    return read_data_to_dataframe(file_path_or_buf, compression="gzip").sort_index()
