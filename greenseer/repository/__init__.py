#  Copyright (c) 2020 GreenSeer (https://chandlersong.me)
#  Copyright (c) 2020 chandler.song
#
#  Licensed under the GNU GENERAL PUBLIC LICENSE v3.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       https://www.gnu.org/licenses/gpl-3.0.html
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

import abc
import logging
import os
import time
from abc import ABC

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


class LocalSource(ABC):
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
    def refresh_data(self, df: DataFrame, stock_id):
        pass

    @abc.abstractmethod
    def exist(self, stock_id):
        """
        check data is dirty or not
        :param stock_id:
        :return:
        """
        pass


class RemoteFetcher(ABC):
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


class ReportLocalData(LocalSource):
    """
    the source is a folder, it work like a table in the database
    each record will be used as a file
    """

    logger = logging.getLogger()

    def __init__(self, source_folder):
        self.__source_folder = source_folder
        if not os.path.exists(self.__source_folder):
            self.logger.info('%s not exists,auto create!!!' % self.__source_folder)
            os.makedirs(self.__source_folder, exist_ok=True)
        else:
            self.logger.info('%s exists' % self.__source_folder)
        self.file_format = source_folder + "/{}.gz"

    @property
    def source_folder(self):
        return self.__source_folder

    def refresh_data(self, df: DataFrame, identify):
        file_path = self.file_format.format(identify)
        if os.path.exists(file_path):
            self.logger.info("{} exists and has been deleted".format(file_path))
            os.remove(file_path)

        df.sort_index().to_csv(file_path, encoding="utf-8", compression="gzip")

    def load_data(self, identify, index_col=0, dtype=None, parse_dates=True, *args, **kwargs) -> DataFrame:
        try:
            return pd.read_csv(self.file_format.format(identify), index_col=index_col, compression="gzip",
                               parse_dates=parse_dates, dtype=dtype)
        except FileNotFoundError:
            self.logger.error("{} not exists in local".format(identify))
            return pd.DataFrame()

    def exist(self, stock_id):
        return os.path.exists(self.file_format.format(stock_id))


class ReportRepository(RemoteFetcher, ABC):
    """
    all the DataSource should be responsibility for one kind of data. like store price.
    if a user need to load different data,it should provide different DataSource.

    and DataSource should can persist data automatically. if it can't find read data from local data source, it will
    load data from remote and persist data to local data source

    Because remote dataStore will be different. so I will list all the common behavior at super class
    and special behavior at  sub class.
    the reason why not use a property is the special thing is a little simple now
    """

    logger = logging.getLogger()

    def __init__(self, local_source: ReportLocalData):
        """

        :param local_source:  it should be a source to persist data
        """
        self.__local_source = local_source

    @property
    def local_source(self) -> LocalSource:
        return self.__local_source

    def load_data(self, stock_id, force_remote=False, remote_delay_max_seconds=None) -> DataFrame:
        # FUTUREIMPROVE:  add dirty check if possible.
        if not self.local_source.exist(stock_id) or force_remote:
            self.logger.info("{} is empty, local data will be refresh".format(stock_id))

            if remote_delay_max_seconds is not None:  # avoid been banned by remote server
                time.sleep(np.random.randint(0, remote_delay_max_seconds))

            remote_data = self.initial_remote_data(stock_id)
            self.local_source.refresh_data(remote_data, stock_id)
            return _change_column_to_datetime(remote_data)
        else:
            return _change_column_to_datetime(self.__local_source.load_data(stock_id))


def _change_column_to_datetime(data: pd.DataFrame):
    data.columns = data.columns.map(pd.Timestamp)
    return data
