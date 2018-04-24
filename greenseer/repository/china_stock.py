import logging
import tempfile
from datetime import datetime

import numpy as np
import tushare as ts
from pandas import DataFrame

from greenseer.configuration import get_global_configuration
from greenseer.repository import BaseRepository, LocalSource, FolderSource, TimeSeriesRemoteFetcher

TU_SHARE_SINA_DAILY = {'amount': np.float64, 'volume': np.float64}


class TuShareHDataFetcher(TimeSeriesRemoteFetcher):
    logger = logging.getLogger()

    def __init__(self, remote_source=ts.get_h_data, sleep_seconds=10, max_random_sleep_seconds=20,
                 remote_fetch_days=365, max_random_remote_fetch_days=35):
        TimeSeriesRemoteFetcher.__init__(self, remote_source, sleep_seconds, max_random_sleep_seconds,
                                         remote_fetch_days,
                                         max_random_remote_fetch_days)

    def load_remote(self, stock_id, start_date, end_date):
        return self.remote_source(stock_id, start=start_date.strftime(DailyPriceRepository.DEFAULT_DATE_FORMAT),
                                  end=end_date.strftime(DailyPriceRepository.DEFAULT_DATE_FORMAT), autype='hfq')

    def get_stock_first_day(self, stock_id):
        return str(ts.get_stock_basics().loc[stock_id]['timeToMarket'])


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
        self.local_source.append_data(stock_id,
                                      self.load_remote(stock_id, next_day, datetime.now()))

    def find_update_date(self, data: DataFrame, today=None):
        if today is None:
            today = datetime.now()

        last_date = data.index[-1]
        if (last_date - today).days == 0:
            return None

        return last_date + self.ONE_DAY




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
