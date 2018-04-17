import logging
import tempfile
import time
from datetime import datetime, timedelta
from random import randint

import numpy as np
import pandas as pd
import tushare as ts
from pandas import DataFrame

from greenseer.repository import BaseRepository, LocalSource, FolderSource

TU_SHARE_SINA_DAILY = {'amount': np.float64, 'volume': np.float64}


class DailyPriceRepository(BaseRepository):
    logger = logging.getLogger()

    ONE_DAY = timedelta(days=1)
    DEFAULT_DATE_FORMAT = '%Y-%m-%d'

    def __init__(self, local_source, remote_source, sleep_seconds=10, remote_fetch_days=365):
        BaseRepository.__init__(self, local_source, remote_source)

        self.__sleep_seconds = sleep_seconds
        self.__remote_fetch_days = remote_fetch_days

        self.logger.info("local source type is {}".format(type(local_source)))
        self.logger.info("remote source type is {}".format(remote_source.__name__))

    def random_sleep_seconds(self):
        result = self.__sleep_seconds + randint(0, 20)
        self.logger.debug("generate sleep {} seconds".format(result))
        return result

    def random_fetch_days(self):
        result = self.__remote_fetch_days + randint(0, 365)
        self.logger.info("generate remote fetch {} days".format(result))
        return timedelta(days=result)

    def load_data(self, stock_id, force_local=False) -> DataFrame:

        local_data = self.local_source.load_data(stock_id).sort_index()
        if local_data.empty or self.check_data_dirty(stock_id, local_data):
            self.logger.info("{} is dirty or empty, local data will be refresh".format(stock_id))
            remote_data = self.initial_remote_data(stock_id)
            self.save_or_update_local(stock_id, remote_data)
            return remote_data
        else:
            self.append_local_if_necessary(stock_id, local_data)
            return local_data

    def find_update_date(self, data: DataFrame, today=None):
        if today is None:
            today = datetime.now()

        last_date = data.index[-1]
        if (last_date - today).days == 0:
            return None

        return last_date + self.ONE_DAY

    def initial_remote_data(self, stock_id) -> DataFrame:
        return self.load_data_by_period(stock_id)

    def load_once(self, stock_id):
        """
        hold for future use
        :param stock_id:
        :return:
        """
        start = datetime.strptime(str(ts.get_stock_basics().loc[stock_id]['timeToMarket']), '%Y%m%d')
        end = datetime.now()
        self.logger.info("start load data, form {} to {}".format(start, end))
        return self.remote_source(stock_id, start=start.strftime(DailyPriceRepository.DEFAULT_DATE_FORMAT),
                                  end=end.strftime(DailyPriceRepository.DEFAULT_DATE_FORMAT))

    def load_data_by_period(self, stock_id):
        period_start_date = datetime.strptime(str(ts.get_stock_basics().loc[stock_id]['timeToMarket']), '%Y%m%d')
        end = datetime.now()
        self.logger.info("start to date: %s" % period_start_date)
        remote_source = self.remote_source
        time.sleep(self.random_sleep_seconds())
        result = []
        is_block = False
        while period_start_date < end:

            try:
                if not is_block:
                    period_end_date = period_start_date + self.random_fetch_days()
                    if period_end_date >= end:
                        period_end_date = end
                else:
                    self.logger.info("last call has been blocked, try again")

                is_block = False

                df = remote_source(stock_id, start=period_start_date.strftime(DailyPriceRepository.DEFAULT_DATE_FORMAT),
                                   end=period_end_date.strftime(DailyPriceRepository.DEFAULT_DATE_FORMAT))

                if df is not None and not df.empty:
                    result.append(df)
                    self.logger.info("load prices,records size{}".format(df.shape))

                    self.logger.info(
                        "load prices:from {} to {},records:{}".format(period_start_date, period_end_date, df.shape))
                period_start_date = period_end_date + DailyPriceRepository.ONE_DAY
                # avoid be block
                time.sleep(self.random_sleep_seconds())
            except IOError as e:
                self.logger.error("has been blocked,will sleep 3 minutes")
                time.sleep(3 * 60)
                is_block = True
                self.logger.error("finish sleep")
                continue

        self.logger.info("finish fetch stock prices: %s" % stock_id)
        if result:  # result is not empty
            return pd.concat(result)
        else:
            return pd.DataFrame()

    def save_or_update_local(self, stock_id, data: DataFrame):
        self.local_source.refresh_data(stock_id, data)

    def check_data_dirty(self, stock_id, local_data: DataFrame):
        latest_record = local_data.tail(1)

        if latest_record.empty:
            return True

        last_date = latest_record.index[0].strftime("%Y-%m-%d")
        remote_record = self.remote_source(stock_id, start=last_date, end=last_date)

        result = not latest_record.sort_index(axis=1).equals(remote_record.sort_index(axis=1))
        self.logger.info("{} local data is dirty".format(stock_id))
        return result

    def append_local_if_necessary(self, stock_id, local_data: DataFrame):
        next_day = self.find_update_date(local_data)

        if next_day is None:
            return

        self.local_source.append_data(stock_id, self.remote_source(stock_id,
                                                                   start=next_day.strftime(
                                                                       DailyPriceRepository.DEFAULT_DATE_FORMAT),
                                                                   end=datetime.now().strftime(
                                                                       DailyPriceRepository.DEFAULT_DATE_FORMAT)))


def create_daily_price_repository(remote_source=None, local_source: LocalSource = None) -> DailyPriceRepository:
    if remote_source is None:
        remote_source = ts.get_h_data

    if local_source is None:
        local_source = FolderSource(tempfile.gettempdir() + "/greenseer")

    return DailyPriceRepository(local_source, remote_source)
