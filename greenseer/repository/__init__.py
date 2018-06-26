import abc
import logging
import os

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
    def load_data(self, *args, **kwargs) -> DataFrame:
        """
        load data from repository
        :param stock_id:
        :param args:
        :param kwargs:
        :return:
        """
        pass

    @abc.abstractmethod
    def append_data(self, new_df: DataFrame, *args, **kwargs):
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
    def refresh_data(self, df: DataFrame, *args, **kwargs):
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

    @abc.abstractmethod
    def initial_remote_data(self, *args, **kwargs):
        """
        initial data. for the first usage
        :param args:
        :param kwargs:
        :return:
        """
        pass

    @abc.abstractmethod
    def load_remote(self, *args, **kwargs):
        """
        load part of data,for update usage
        :param args:
        :param kwargs:
        :return:
        """
        pass

    @abc.abstractmethod
    def check_data_dirty(self, stock_id, local_data: DataFrame):
        """
        check data is dirty or not
        :param stock_id:
        :param local_data:
        :return:
        """
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
        self.local_source.refresh_data(remote_data, stock_id)

    @abc.abstractmethod
    def append_local_if_necessary(self, stock_id, local_data: DataFrame):
        pass


class RemoteBaseRepository(BaseRepository):
    logger = logging.getLogger("remoteBaseRepository")

    def __init__(self):
        BaseRepository.__init__(self, None)

    def load_data(self, stock_id, force_local=False) -> DataFrame:
        return self.load_remote(stock_id)


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
        self.file_format = source_folder + "/{}.gz"

    @property
    def source_folder(self):
        return self.__source_folder

    def refresh_data(self, df: DataFrame, stock_id, *args, **kwargs):
        file_path = self.file_format.format(stock_id)
        if os.path.exists(file_path):
            self.logger.info("{} exists and has been deleted".format(file_path))
            os.remove(file_path)

        df.sort_index().to_csv(file_path, encoding="utf-8", compression="gzip")

    def append_data(self, new_df: DataFrame, stock_id, *args, **kwargs):
        self.refresh_data(self.load_data(stock_id).append(new_df), stock_id)

    def load_data(self, stock_id, *args, **kwargs) -> DataFrame:
        try:
            return pd.read_csv(self.file_format.format(stock_id), index_col=0, compression="gzip", parse_dates=True)
        except FileNotFoundError:
            self.logger.error("{} not exists in local".format(stock_id))
            return pd.DataFrame()


class FileSource(LocalSource):
    """
    the source is a folder, it work like a table in the database
    each record will be used as a file
    """

    logger = logging.getLogger()

    def __init__(self, source_path):

        self.__source_path = source_path

        if not os.path.exists(source_path):
            self.logger.info("{} not exits".format(source_path))
            self.__cache = DataFrame()
            return

        self.__cache = pd.read_csv(source_path, dtype={"code": np.str}, compression="gzip").set_index(
            "code").sort_index()

    @property
    def cache(self):
        return self.__cache

    @property
    def cache_enabled(self):
        return not self.__cache.empty

    def load_data(self, stock_id, *args, **kwargs) -> DataFrame:
        try:
            return self.cache.loc[[stock_id]]
        except KeyError:
            return DataFrame()

    def append_data(self, new_df: DataFrame, *args, **kwargs):
        self.refresh_data(new_df)

    def refresh_data(self, df: DataFrame, *args, **kwargs):

        if os.path.exists(self.__source_path):
            self.logger.info("{} exits,has been removed".format(self.__source_path))
            os.remove(self.__source_path)

        df.to_csv(self.__source_path, compression="gzip")
        self.__cache = df.copy()
