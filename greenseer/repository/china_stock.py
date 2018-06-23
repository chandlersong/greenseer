import logging
import tempfile
from datetime import datetime
from urllib import request

import numpy as np
import pandas as pd
import tushare as ts
from pandas import DataFrame

from greenseer.configuration import get_global_configuration
from greenseer.repository import BaseRepository, LocalSource, FolderSource, TimeSeriesRemoteFetcher, RemoteFetcher, \
    RemoteBaseRepository

TU_SHARE_SINA_DAILY = {'amount': np.float64, 'volume': np.float64}


class TuShareHDataFetcher(TimeSeriesRemoteFetcher):
    logger = logging.getLogger()

    def __init__(self, remote_source=ts.get_h_data, sleep_seconds=10, max_random_sleep_seconds=20,
                 remote_fetch_days=365, max_random_remote_fetch_days=35):
        TimeSeriesRemoteFetcher.__init__(self, remote_source, sleep_seconds, max_random_sleep_seconds,
                                         remote_fetch_days,
                                         max_random_remote_fetch_days)

    def do_load_remote(self, stock_id, start_date, end_date, *args, **kwargs):
        return self.remote_source(stock_id, start=start_date.strftime(DailyPriceRepository.DEFAULT_DATE_FORMAT),
                                  end=end_date.strftime(DailyPriceRepository.DEFAULT_DATE_FORMAT), autype='hfq')

    def get_stock_first_day(self, stock_id) -> datetime:
        return datetime.strptime(str(ts.get_stock_basics().loc[stock_id]['timeToMarket']), '%Y%m%d')


class TuShareStockBasicFetcher(RemoteFetcher):

    def __init__(self):
        self.__cache = None

    @property
    def cache(self):
        return self.__cache

    @property
    def all_stock_basic_info(self) -> DataFrame:
        if self.cache is None:
            self.initial_remote_data()
        return self.cache

    def initial_remote_data(self):
        self.__cache = ts.get_stock_basics()
        return self.__cache

    def load_remote(self, stock_id):
        if self.cache is None:
            self.initial_remote_data()
        return self.cache.loc[[stock_id]]

    def check_data_dirty(self, stock_id, local_data: DataFrame):
        """
        Becasue if check the data is dirty or not, it will need to load all the data from remote. so just
        the system to refresh all the time

        :param stock_id:
        :param local_data:
        :return:
        """
        return False


class DailyPriceRepository(BaseRepository, TuShareHDataFetcher):
    logger = logging.getLogger()

    DEFAULT_DATE_FORMAT = '%Y-%m-%d'

    def __init__(self, local_source, remote_source, sleep_seconds=10, max_random_sleep_seconds=20,
                 remote_fetch_days=365, max_random_remote_fetch_days=35):
        BaseRepository.__init__(self, local_source)
        TuShareHDataFetcher.__init__(self, remote_source, sleep_seconds, max_random_sleep_seconds, remote_fetch_days,
                                     max_random_remote_fetch_days)
        self.logger.info("local source type is {}".format(type(local_source)))

    def append_local_if_necessary(self, stock_id, local_data: DataFrame):
        next_day = self.find_update_date(local_data)

        if next_day is None:
            self.logger.info("no need to append")
            return
        self.logger.info("next day is {}".format(next_day))
        self.local_source.append_data(self.load_remote(stock_id, next_day, datetime.now())
                                      , stock_id)

    def find_update_date(self, data: DataFrame, today=None):
        if today is None:
            today = datetime.now()

        last_date = data.index[-1]
        if (last_date - today).days == 0:
            return None

        return last_date + self.ONE_DAY


class ChinaAssertRepository(BaseRepository):
    INDEX_COL = 0

    ZERO_NA_VALUE = 0

    logger = logging.getLogger()

    def __init__(self, local_repository):
        BaseRepository.__init__(self, local_repository)
        self.remote_path = 'http://quotes.money.163.com/service/zcfzb_{}.html'

    def initial_remote_data(self, stock_id):
        path = self.remote_path.format(stock_id)
        self.logger.debug("file path is %s", path)
        with request.urlopen(path) as web:
            local = pd.read_csv(web, encoding='gb2312', na_values='--', index_col=ChinaAssertRepository.INDEX_COL)
            return local.drop(local.columns[len(local.columns) - 1], axis=1).fillna(
                ChinaAssertRepository.ZERO_NA_VALUE).apply(pd.to_numeric,
                                                           errors='coerce')

    def load_remote(self, *args, **kwargs):
        pass

    def check_data_dirty(self, stock_id, local_data: DataFrame):
        return False;

    def append_local_if_necessary(self, stock_id, local_data: DataFrame):
        pass


class BasicInfoRepository(RemoteBaseRepository, TuShareStockBasicFetcher):

    def __init__(self):
        TuShareStockBasicFetcher.__init__(self)

    def append_local_if_necessary(self, stock_id, local_data: DataFrame):
        pass


def create_daily_price_repository(remote_source=None, local_source: LocalSource = None) -> DailyPriceRepository:
    if remote_source is None:
        remote_source = ts.get_h_data

    if local_source is None:
        local_source = FolderSource(tempfile.gettempdir() + "/greenseer/china_stock_price_daily")
    global_config = get_global_configuration()
    return DailyPriceRepository(local_source,
                                remote_source,
                                sleep_seconds=global_config.get_int_value("china_stock_config", "sleep_seconds", 10),
                                max_random_sleep_seconds=global_config.get_int_value("china_stock_config",
                                                                                     "max_random_sleep_seconds", 20),
                                remote_fetch_days=global_config.get_int_value("china_stock_config", "remote_fetch_days",
                                                                              365),
                                max_random_remote_fetch_days=global_config.get_int_value("china_stock_config",
                                                                                         "max_random_remote_fetch_days",
                                                                                         35))


Gobal_BASIC_INFO_REPOSITORY = BasicInfoRepository()


def get_gobal_basic_info_repository() -> Gobal_BASIC_INFO_REPOSITORY:
    return Gobal_BASIC_INFO_REPOSITORY


def create_china_assert_repository(local_source=None) -> ChinaAssertRepository:
    if local_source is None:
        local_source = FolderSource(tempfile.gettempdir() + "/greenseer/china_assert_reports")

    return ChinaAssertRepository(local_source)
