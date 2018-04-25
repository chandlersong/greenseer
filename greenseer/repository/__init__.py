import abc
import logging
import os
import time
from datetime import timedelta, datetime
from random import randint

import numpy as np
import pandas as pd
from pandas import DataFrame

"""
this module is a data source
I hope it could be a smart one. it can choice to pick data from local or remote.

the logic need to detect local data is dirty. if the data is dirty, it will re-refresh all the data.
Currently, there's two type data.
one is TimeSeries,like stock prices. date and stock id is the primary key
another is stockinfo, like stock information, stock id is the primary key.

each will have it's own implementation.

"""

DATA_TYPE_FOR_TRANSFORM = {'amount': np.float64, 'volume': np.float64}


class LocalSource:
    """
    this is a interface. Because I am familiar with interface in java. I use a stupid solution here.
    need to be changed if there's a good solution
    """

    @abc.abstractmethod
    def load_data(self, stock_id, *args, **kwargs) -> DataFrame:
        """
        load data from repository
        :param stock_id:
        :param args:
        :param kwargs:
        :return:
        """
        pass

    @abc.abstractmethod
    def initial_data(self, stock_id, df: DataFrame, *args, **kwargs):
        """
        initial data, there should be no data before
        :param stock_id:
        :param df:
        :param args:
        :param kwargs:
        :return:
        """
        pass

    @abc.abstractmethod
    def append_data(self, stock_id, new_df: DataFrame, *args, **kwargs):
        """
        add new data to the tail
        :param stock_id:
        :param df:
        :param args:
        :param kwargs:
        :return:
        """
        pass

    @abc.abstractmethod
    def refresh_data(self, stock_id, df: DataFrame, *args, **kwargs):
        """
        refresh data, this method will clean up all the old data
        if you want to keep old data, please use append_data
        :param stock_id:
        :param df:
        :param args:
        :param kwargs:
        :return:
        """
        pass


class RemoteFetcher:
    logger = logging.getLogger()

    def __init__(self, remote_source):
        self.__remote_source = remote_source
        self.logger.info("remote source type is {}".format(remote_source.__name__))

    @property
    def remote_source(self):
        return self.__remote_source

    @abc.abstractmethod
    def initial_remote_data(self, *args, **kwargs):
        pass

    @abc.abstractmethod
    def load_remote(self, *args, **kwargs):
        pass

    @abc.abstractmethod
    def check_data_dirty(self, stock_id, local_data: DataFrame):
        pass


class BaseRepository(RemoteFetcher):
    '''
    all the DataSource should be responsibility for one kind of data. like store price.
    if a user need to load different data,it should provide different DataSource.

    and DataSource should can persist data automatically. if it can't find read data from local data source, it will
    load data from remote and persist data to local data source

    Because remote dataStore will be different. so I will list all the common behavior at super class
    and special behavior at  sub class.
    the reason why not use a property is the special thing is a little simple now
    '''

    logger = logging.getLogger()

    def __init__(self, local_source):
        """

        :param local_source:  it should be a source to persist data
        :param remote_source: it should be a function. be called
        """
        self.__local_source = local_source

    @property
    def local_source(self) -> LocalSource:
        return self.__local_source

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

    def save_or_update_local(self, stock_id, remote_data: DataFrame):
        self.local_source.refresh_data(stock_id, remote_data)

    @abc.abstractmethod
    def append_local_if_necessary(self, stock_id, local_data: DataFrame):
        pass


class TimeSeriesRemoteFetcher(RemoteFetcher):
    logger = logging.getLogger()

    ONE_DAY = timedelta(days=1)

    def __init__(self, remote_source, sleep_seconds=10, max_random_sleep_seconds=20,
                 remote_fetch_days=365, max_random_remote_fetch_days=35,
                 block_sleep_seconds=3 * 60):
        RemoteFetcher.__init__(self, remote_source)
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

    def initial_remote_data(self, *args, **kwargs):
        pass

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
    def get_stock_first_day(self, stock_id):
        pass

    def check_data_dirty(self, stock_id, local_data: DataFrame):
        latest_record = local_data.tail(1)

        if latest_record.empty:
            return True

        last_date = latest_record.index[0]
        remote_record = self.load_remote(stock_id, last_date, last_date)

        result = not latest_record.sort_index(axis=1).equals(remote_record.sort_index(axis=1))
        self.logger.info("{} local data is dirty".format(stock_id))
        return result

    def initial_remote_data(self, stock_id) -> DataFrame:
        return self.load_data_by_period(stock_id)

    def load_data_by_period(self, stock_id):
        period_start_date = datetime.strptime(self.get_stock_first_day(stock_id), '%Y%m%d')
        end = datetime.now()
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


class FolderSource(LocalSource):
    """
    the source is a folder, it work like a table in the database
    each record will be used as a file
    """

    logger = logging.getLogger()

    def __init__(self, source_folder):
        self.__source_folder = source_folder
        if not os.path.exists(self.__source_folder):
            self.logger.info('%s not exists,auto create!!!' % (self.__source_folder))
            os.makedirs(self.__source_folder, exist_ok=True)
        else:
            self.logger.info('%s exists' % self.__source_folder)
        self.file_format = source_folder + "/{}.tar.gz"

    @property
    def source_folder(self):
        return self.__source_folder

    def refresh_data(self, stock_id, df: DataFrame, *args, **kwargs):
        file_path = self.file_format.format(stock_id)
        if os.path.exists(file_path):
            self.logger.info("{} exists and has been deleted".format(file_path))
            os.remove(file_path)

        df.sort_index().to_csv(file_path, compression="gzip")

    def append_data(self, stock_id, new_df: DataFrame, *args, **kwargs):
        self.refresh_data(stock_id, self.load_data(stock_id).append(new_df))

    def load_data(self, stock_id, *args, **kwargs) -> DataFrame:
        try:
            return pd.read_csv(self.file_format.format(stock_id), index_col=0, compression="gzip", parse_dates=True)
        except FileNotFoundError:
            self.logger.error("{} not exists in local".format(stock_id))
            return pd.DataFrame()

    def initial_data(self, stock_id, df: DataFrame):
        file_path = self.file_format.format(stock_id)
        df.to_csv(file_path, compression="gzip")
