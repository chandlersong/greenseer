import abc
import logging
import tempfile
import time
from datetime import datetime, timedelta
from random import randint
from urllib import request

import numpy as np
import pandas as pd
import tushare as ts
from pandas import DataFrame

from greenseer.configuration import get_global_configuration
from greenseer.repository import BaseRepository, LocalSource, FolderSource, RemoteFetcher, \
    RemoteBaseRepository

TU_SHARE_SINA_DAILY = {'amount': np.float64, 'volume': np.float64}


class TuShareStockBasicFetcher(RemoteFetcher):

    def __init__(self, remote_source):
        self.__remote_source = remote_source
        self.logger.info("remote source type is {}".format(remote_source.__name__))

        self.__cache = None

    @property
    def remote_source(self):
        return self.__remote_source

    @property
    def cache(self):
        return self.__cache

    @property
    def all_stock_basic_info(self) -> DataFrame:
        if self.cache is None:
            self.initial_remote_data()
        return self.cache

    def initial_remote_data(self):
        pass

    def load_remote(self, stock_id):
        if self.cache is None:
            self.__cache = self.initial_remote_data()
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


class TimeSeriesRemoteFetcher(TuShareStockBasicFetcher):
    logger = logging.getLogger()

    ONE_DAY = timedelta(days=1)

    def __init__(self, remote_source, sleep_seconds=10, max_random_sleep_seconds=20,
                 remote_fetch_days=365, max_random_remote_fetch_days=35,
                 block_sleep_seconds=3 * 60):
        TuShareStockBasicFetcher.__init__(self, remote_source)
        self.__sleep_seconds = sleep_seconds
        self.__remote_fetch_days = remote_fetch_days
        self.__max_random_sleep_seconds = max_random_sleep_seconds
        self.__max_random_remote_fetch_days = max_random_remote_fetch_days
        self.__block_sleep_seconds = block_sleep_seconds

        self.logger.info("block sleep seconds is {}".format(block_sleep_seconds))

        self.logger.info("sleep seconds is between {} to {}".format(self.__sleep_seconds,
                                                                    self.__sleep_seconds +
                                                                    self.__max_random_sleep_seconds))
        self.logger.info("fetch days is between  {} to {} ".format(self.__remote_fetch_days,
                                                                   self.__remote_fetch_days +
                                                                   self.__max_random_remote_fetch_days))

    def random_sleep_seconds(self):
        result = self.__sleep_seconds + randint(0, self.__max_random_sleep_seconds)
        self.logger.debug("generate sleep {} seconds".format(result))
        return result

    def random_fetch_days(self):
        result = self.__remote_fetch_days + randint(0, self.__max_random_remote_fetch_days)
        self.logger.debug("generate remote fetch {} days".format(result))
        return timedelta(days=result)

    def load_remote(self, stock_id, start_date, end_date, *args, **kwargs):
        try:
            return self.do_load_remote(stock_id, start_date, end_date, *args, **kwargs)
        except IOError:
            self.logger.error("has been blocked,will sleep 3 minutes")
            time.sleep(self.__block_sleep_seconds)
            self.logger.error("finish sleep")
            return self.load_remote(stock_id, start_date, end_date)

    @abc.abstractmethod
    def do_load_remote(self, stock_id, start_date, end_date, *args, **kwargs):
        pass

    @abc.abstractmethod
    def get_stock_first_day(self, stock_id) -> datetime:
        pass

    def check_data_dirty(self, stock_id, local_data: DataFrame):
        latest_record = local_data.tail(1)

        if latest_record.empty:
            return True

        last_date = latest_record.index[0]
        remote_record = self.load_remote(stock_id, last_date, last_date)

        result = not latest_record.sort_index(axis=1).equals(remote_record.sort_index(axis=1))
        self.logger.info("{} local data is dirty :{}".format(stock_id, result))
        return result

    def initial_remote_data(self, stock_id) -> DataFrame:
        return self.load_data_by_period(stock_id)

    def load_data_by_period(self, stock_id):
        period_start_date = self.get_stock_first_day(stock_id)
        end = datetime.today()
        self.logger.info("start to date: %s" % period_start_date)
        time.sleep(self.random_sleep_seconds())
        result = []
        while period_start_date < end:

            period_end_date = period_start_date + self.random_fetch_days()
            if period_end_date >= end:
                period_end_date = end

            df = self.load_remote(stock_id, period_start_date, period_end_date)

            if df is not None and not df.empty:
                result.append(df)

                self.logger.info(
                    "load prices:from {} to {},records shape:{}".format(period_start_date, period_end_date,
                                                                        df.shape))
            period_start_date = period_end_date + TimeSeriesRemoteFetcher.ONE_DAY
            # avoid be block
            time.sleep(self.random_sleep_seconds())

        self.logger.info("finish fetch stock prices: %s" % stock_id)
        if result:  # result is not empty
            return pd.concat(result).sort_index()
        else:
            return pd.DataFrame()


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

        self.__check_clean_cache = None

    @property
    def check_clean_cache(self):
        '''
        In some case, when to check data is dirty or not, will totally refresh the data. so use it to cache.
        :return:
        '''
        return self.__remote_source

    @check_clean_cache.setter
    def set_check_clean_cache(self, cache: DataFrame):
        self.__check_clean_cache = cache

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
        TuShareStockBasicFetcher.__init__(self, ts.get_stock_basics)

    def append_local_if_necessary(self, stock_id, local_data: DataFrame):
        pass

    def initial_remote_data(self):
        self.__cache = ts.get_stock_basics()
        return self.__cache


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
