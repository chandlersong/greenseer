#  Copyright (c) 2020 RumorMill (https://chandlersong.me)
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

import logging
from urllib import request

import numpy as np
import pandas as pd
import tushare as ts
from pandas import DataFrame

from greenseer.repository import ReportRepository, ReportLocalData, RemoteFetcher

NET_EASE_ENCODE = 'gb2312'

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


class NetEaseRemoteFetcher(RemoteFetcher):

    def __init__(self, remote_path_format):
        self.__remote_path_format = remote_path_format
        self.logger.debug("remote path format is %s", remote_path_format)

    def initial_remote_data(self, stock_id):
        path = self.__remote_path_format.format(stock_id)
        self.logger.debug("file path is %s", path)
        with request.urlopen(path) as web:
            local = pd.read_csv(web, encoding=NET_EASE_ENCODE, na_values='--',
                                index_col=ChinaAssertRepository.INDEX_COL)
            return local.drop(local.columns[len(local.columns) - 1], axis=1).fillna(
                ChinaAssertRepository.ZERO_NA_VALUE).apply(pd.to_numeric,
                                                           errors='coerce')

    def load_remote(self, stock_id):
        return self.initial_remote_data(stock_id)


class ChinaAssertRepository(ReportRepository, NetEaseRemoteFetcher):
    INDEX_COL = 0

    ZERO_NA_VALUE = 0

    logger = logging.getLogger()

    def __init__(self, local_repository):
        ReportRepository.__init__(self, local_repository)
        NetEaseRemoteFetcher.__init__(self, 'http://quotes.money.163.com/service/zcfzb_{}.html')

    def load_data(self, stock_id, force_remote=False, remote_delay_max_seconds=None) -> DataFrame:
        return super().load_data(stock_id, force_remote, remote_delay_max_seconds).T


class ChinaCashRepository(ReportRepository, NetEaseRemoteFetcher):
    INDEX_COL = 0

    ZERO_NA_VALUE = 0

    logger = logging.getLogger()

    def __init__(self, local_repository):
        ReportRepository.__init__(self, local_repository)
        NetEaseRemoteFetcher.__init__(self, 'http://quotes.money.163.com/service/xjllb_{}.html')

    def load_data(self, stock_id, force_remote=False, remote_delay_max_seconds=None) -> DataFrame:
        return super().load_data(stock_id, force_remote, remote_delay_max_seconds).T


class ChinaIncomeRepository(ReportRepository, NetEaseRemoteFetcher):
    INDEX_COL = 0

    ZERO_NA_VALUE = 0

    logger = logging.getLogger()

    def __init__(self, local_repository):
        ReportRepository.__init__(self, local_repository)
        NetEaseRemoteFetcher.__init__(self, 'http://quotes.money.163.com/service/lrb_{}.html')

    def load_data(self, stock_id, force_remote=False, remote_delay_max_seconds=None) -> DataFrame:
        return super().load_data(stock_id, force_remote, remote_delay_max_seconds).T


class BasicInfoRepository(TuShareStockBasicFetcher):
    def __init__(self):
        TuShareStockBasicFetcher.__init__(self, ts.get_stock_basics)
        self.__cache = None

    def initial_remote_data(self):
        self.__cache = ts.get_stock_basics()
        return self.__cache

    def load_data(self, __stock_id):
        if self.__cache is None:
            self.initial_remote_data()

        return self.__cache


Global_BASIC_INFO_REPOSITORY = BasicInfoRepository()


def get_global_basic_info_repository() -> Global_BASIC_INFO_REPOSITORY:
    return Global_BASIC_INFO_REPOSITORY


def create_china_stock_assert_repository(local_source=None, base_folder="reportData") -> ChinaAssertRepository:
    if local_source is None:
        local_source = ReportLocalData(base_folder + "/greenseer/china_assert_reports")

    return ChinaAssertRepository(local_source)


def create_china_stock_cash_repository(local_source=None, base_folder="reportData") -> ChinaCashRepository:
    if local_source is None:
        local_source = ReportLocalData(base_folder + "/greenseer/china_cash_reports")

    return ChinaCashRepository(local_source)


def create_china_stock_income_repository(local_source=None, base_folder="reportData") -> ChinaIncomeRepository:
    if local_source is None:
        local_source = ReportLocalData(base_folder + "/greenseer/china_income_reports")

    return ChinaIncomeRepository(local_source)
