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
import logging
from functools import partial

import numpy as np
import pandas as pd
import tushare as ts
from sklearn.model_selection import train_test_split

from greenseer.repository import ReportLocalData
from greenseer.repository.china_stock import create_china_stock_assert_repository, create_china_stock_income_repository, \
    create_china_stock_cash_repository, ChinaAssertRepository, ChinaIncomeRepository, ChinaCashRepository, \
    get_global_basic_info_repository

ASSERT_REPORT = "assert"

INCOME_REPORT = "income"

CASH_REPORT = "cash"

DEFAULT_LOCAL_PATH = "reportData"

TRAIN_SET_ALL = None

CODE_INDEX_NAME = "code"

RELEASE_AT_INDEX_NAME = "releaseAt"


class ChinaReportRepository:
    _fields = ["_assert", "_income", "_cash", "_stock_info", "_local_path"]

    def __init__(self):
        self._stock_info = None
        self._assert = None
        self._cash = None
        self._income = None
        self._local_path = DEFAULT_LOCAL_PATH
        self.refresh(DEFAULT_LOCAL_PATH)

    def refresh(self, local_path):
        self._stock_info = get_global_basic_info_repository()
        self._assert = create_china_stock_assert_repository(base_folder=local_path)
        self._income = create_china_stock_income_repository(base_folder=local_path)
        self._cash = create_china_stock_cash_repository(base_folder=local_path)
        self._local_path = local_path

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

    @property
    def local_path(self) -> str:
        return self._local_path


_repository = ChinaReportRepository()

_logger = logging.getLogger()

_local_all_reports_repo = ReportLocalData(DEFAULT_LOCAL_PATH + "/chinaReports")

_ALL_REPORTS_NAME = "all_finance_reports"


def stock_info(repository=_repository) -> pd.DataFrame:
    return repository.stock_info


def set_local_path(local_path: str):
    _repository.refresh(local_path)
    global _local_all_reports_repo
    _local_all_reports_repo = ReportLocalData(local_path + "/chinaReports")


def load_train_data(train_size=10, reload=False, force_remote=False, repository=_repository) -> (
        pd.DataFrame, pd.DataFrame):
    """
    FUTUREIMPROVE: add target here

    the trains_size and test_size, please refer to the train_test_split in sklearn
    :return: Train set, Test test
    """
    stock_ids = repository.stock_info.index.astype(str)

    if train_size is not None:
        train_index, _ = train_test_split(stock_ids, train_size=train_size, test_size=0)
        return fetch_multi_report(train_index)
    else:
        return fetch_all(reload=reload, force_remote=force_remote)


def load_multi_data(stock_ids: np.array, force_remote=False, repository=_repository,
                    max_sleep_seconds=5) -> pd.DataFrame:
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


def fetch_all(reload=False, force_remote=False, repository=_repository, max_sleep_seconds=5) -> pd.DataFrame:
    """
    this is only for load all stock info convenience. and it will take hours if you use all default for the first time.

    reload is compose the all report in local. and force_remote will fetch the data from remote.
    so you can update some data and call remote=True and force_remote=False and don't need to fetch whole from remote again

    :param reload: reload local repository or not
    :param force_remote: force to fetch from remote
    :param repository:  repository
    :param max_sleep_seconds: sleep seconds for each call remote
    :return:
    """
    if not reload and not force_remote:
        result = __load_all_locally()
        if result.empty:
            result = __reload_all_locally(force_remote, repository, max_sleep_seconds)
            result = result.rename_axis([CODE_INDEX_NAME, RELEASE_AT_INDEX_NAME])
            data = result.reset_index()
            data = data.astype({"code": str})
            _local_all_reports_repo.refresh_data(data, _ALL_REPORTS_NAME)
        return result
    else:
        result = __reload_all_locally(force_remote, repository, max_sleep_seconds)
        _local_all_reports_repo.refresh_data(result, _ALL_REPORTS_NAME)


def __reload_all_locally(force_remote, repository, max_sleep_seconds) -> pd.DataFrame:
    info = repository.stock_info
    index = info.index
    _logger.info("total will fetch {} stocks report".format(len(index)))
    return fetch_multi_report(index, force_remote=force_remote, max_sleep_seconds=max_sleep_seconds)


def __load_all_locally() -> pd.DataFrame:
    data = _local_all_reports_repo.load_data(_ALL_REPORTS_NAME, dtype={"code": str})
    if not data.empty:
        data = data.set_index([CODE_INDEX_NAME, RELEASE_AT_INDEX_NAME])
    return data


def compose_target(target_info: dict, index: pd.Index, level=None) -> pd.DataFrame:
    """
    compose the target set. because stock can be divide according to many different reason, it only compose by
    the information provide by users. and it will provide some default groups in other method˚

    :param level: index level
    :param target_info: a dict which list the type of stock id. like {"st":[stockid1,stockid2]}
    :param index: target index
    :return:
    """
    result = pd.DataFrame(index=index)

    for key, value in target_info.items():
        result[key] = result.index.isin(value, level=level).astype(int)

    return result


def list_default_targets() -> dict:
    result = dict()
    st_stocks = ts.get_st_classified()
    result["st"] = st_stocks["code"].values
    return result


def list_industry_category(repository: ChinaReportRepository = _repository, industry_column="industry") -> dict:
    return {key: list(value) for key, value in repository.stock_info.groupby(industry_column).groups.items()}


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
fetch_default_targets = partial(compose_target, target_info=list_default_targets(), index=_repository.stock_info.index)
fetch_targets = partial(compose_target, target_info=list_default_targets())
