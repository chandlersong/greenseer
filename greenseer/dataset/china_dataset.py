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

from functools import partial

import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split

from greenseer.repository.china_stock import create_china_stock_assert_repository, create_china_stock_income_repository, \
    create_china_stock_cash_repository, ChinaAssertRepository, ChinaIncomeRepository, ChinaCashRepository, \
    get_global_basic_info_repository

ASSERT_REPORT = "assert"

INCOME_REPORT = "income"

CASH_REPORT = "cash"

DEFAULT_LOCAL_PATH = "reportData"


class ChinaReportRepository:
    _fields = ["_assert", "_income", "_cash", "_stock_info"]

    def __init__(self):
        self._stock_info = None
        self._assert = None
        self._cash = None
        self._income = None
        self.refresh(DEFAULT_LOCAL_PATH)

    def refresh(self, local_path):
        self._stock_info = get_global_basic_info_repository()
        self._assert = create_china_stock_assert_repository(base_folder=local_path)
        self._income = create_china_stock_income_repository(base_folder=local_path)
        self._cash = create_china_stock_cash_repository(base_folder=local_path)

    @property
    def assert_report(self) -> ChinaAssertRepository:
        return self._assert

    @property
    def income_report(self) -> ChinaIncomeRepository:
        return self._income

    @property
    def cash_report(self) -> ChinaCashRepository:
        return self._cash

    @property
    def stock_info(self) -> pd.DataFrame:
        return self._stock_info.load_data()


_repository = ChinaReportRepository()


def set_local_path(local_path: str):
    _repository.refresh(local_path)


def load_train_data(train_size=10, test_size=2, repository=_repository) -> (pd.DataFrame, pd.DataFrame):
    """
    FUTUREIMPROVE: add target here

    the trains_size and test_size, please refer to the train_test_split in sklearn
    :return: Train set, Test test
    """
    stock_ids = repository.stock_info.index.astype(str)
    train_index, test_index = train_test_split(stock_ids, train_size=train_size, test_size=test_size)

    train_data = fetch_multi_report(train_index)
    test_data = fetch_multi_report(test_index)
    return train_data, test_data


def load_multi_data(stock_ids: np.array, force_remote=False, repository=_repository,
                    max_sleep_seconds=9) -> pd.DataFrame:
    """
    the main purpose is for load stock data in batch for ml.
    so I will try split data into here

    :param max_sleep_seconds:
    :param stock_ids: stock id list
    :param force_remote: force to load from remote
    :param repository: repository
    :return:
    """

    return pd.concat([load_by_stock_id(stock, force_remote, repository, max_sleep_seconds) for stock in stock_ids])


def load_by_stock_id(stock_id: str, force_remote=False, repository=_repository, max_sleep_seconds=9) -> pd.DataFrame:
    assert_report = repository.assert_report.load_data(stock_id=stock_id, force_remote=force_remote,
                                                       remote_delay_max_seconds=max_sleep_seconds)
    income_report = repository.income_report.load_data(stock_id=stock_id, force_remote=force_remote,
                                                       remote_delay_max_seconds=max_sleep_seconds)
    cash_report = repository.cash_report.load_data(stock_id=stock_id, force_remote=force_remote,
                                                   remote_delay_max_seconds=max_sleep_seconds)
    return pd.concat({stock_id: pd.concat([assert_report, income_report, cash_report], axis=1)})


fetch_one_report = partial(load_by_stock_id, repository=_repository)
fetch_multi_report = partial(load_multi_data, repository=_repository)
fetch_train_set = partial(load_train_data, repository=_repository)
