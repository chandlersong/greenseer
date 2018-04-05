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


def change_index_to_date_type(func):
    def format_index(*args, **kwargs):
        result = func(*args, **kwargs)
        index = result.index
        return result.set_index(index.set_names(['date']).to_datetime())

    return format_index


def change_index_to_str(func):
    def format_index(*args, **kwargs):
        result = func(*args, **kwargs)
        return result.set_index(pd.Index(result.index.map(str)))

    return format_index


class BaseRepository:
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

    def __init__(self, local_source, remote_source):
        """

        :param local_source:  it should be a source to persist data
        :param remote_source: it should be a function. be called
        """
        self.__local_source = local_source
        self.__remote_source = remote_source

    @property
    def local_source(self):
        return self.__local_source

    @property
    def remote_source(self):
        return self.__remote_source

    def load_data(self, stock_id, force_local=False, *args, **kwargs):
        """

        """
        if not force_local:
            if self.need_initial(stock_id, *args, **kwargs):
                '''
                if data is dirty, return new data and update local database
               '''
                self.logger.debug('data is dirty, refresh data,*args:%s,**kwargs:%s' % (args, kwargs))
                self.__local_source.initial_data(self.fetch_initial_data(stock_id, *args, **kwargs), *args, **kwargs)
            else:
                self.logger.debug('data is clean, start to update,*args:%s,**kwargs:%s' % (args, kwargs))
                self.update_data(stock_id, *args, **kwargs)

        return self.__local_source.load_data(stock_id, dtype=self.get_dtype(), *args, **kwargs)

    def need_initial(self, stock_id, *args, **kwargs):
        """
        check need to do initial or not
        :return: need to do the initial or not
        """
        pass

    def fetch_initial_data(self, stock_id, *args, **kwargs):
        pass

    def update_data(self, stock_id, *args, **kwargs):
        pass

    def get_dtype(self):
        pass


class LocalSource:
    """
    this is a interface. Because I am familiar with interface in java. I use a stupid solution here.
    need to be changed if there's a good solution
    """

    @abc.abstractmethod
    def load_data(self, stock_id, *args, **kwargs):
        """
        load data from repository
        :param stock_id:
        :param args:
        :param kwargs:
        :return:
        """
        pass

    @abc.abstractmethod
    def initial_data(self, stock_id, df:DataFrame, *args, **kwargs):
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
    def append_data(self, stock_id, new_df:DataFrame, *args, **kwargs):
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
    def refresh_data(self, stock_id, df:DataFrame, *args, **kwargs):
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
            self.logger.info('%s exists' % (self.__source_folder))
        self.file_format = source_folder + "/{}.tar.gz"

    @property
    def source_folder(self):
        return self.__source_folder

    def refresh_data(self, stock_id, df:DataFrame, *args, **kwargs):
        file_path = self.file_format.format(stock_id)
        if os.path.exists(file_path):
            self.logger.info("{} exists and has been deleted".format(file_path))
            os.remove(file_path)

        df.to_csv(file_path, compression="gzip")

    def append_data(self, stock_id, new_df: DataFrame, *args, **kwargs):
        self.refresh_data(stock_id, self.load_data(stock_id).append(new_df))

    def load_data(self, stock_id, *args, **kwargs) -> DataFrame:
        try:
            return pd.read_csv(self.file_format.format(stock_id), index_col=0, compression="gzip", parse_dates=True)
        except FileNotFoundError:
            self.logger.error("{} not exists, return empty".format(stock_id))
            return pd.DataFrame()

    def initial_data(self, stock_id, df: DataFrame):
        file_path = self.file_format.format(stock_id)
        df.to_csv(file_path, compression="gzip")


class InitialTool:
    def initial(self, *args, **kwargs):
        pass
