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

import pandas as pd

from greenseer.repository.china_stock import create_china_stock_assert_repository, create_china_stock_income_repository, \
    create_china_stock_cash_repository, ChinaAssertRepository, ChinaIncomeRepository, ChinaCashRepository

DEFAULT_LOCAL_PATH = "reportData"


class _MyRepository:
    _fields = ["_assert", "_income", "_cash"]

    def __init__(self):
        self._assert = None
        self._cash = None
        self._income = None
        self.refresh(DEFAULT_LOCAL_PATH)

    def refresh(self, local_path):
        self._assert = create_china_stock_assert_repository(base_folder=local_path)
        self._income = create_china_stock_income_repository(base_folder=local_path)
        self._cash = create_china_stock_cash_repository(base_folder=local_path)

    @property
    def assertReport(self) -> ChinaAssertRepository:
        return self._assert

    @property
    def incomeReport(self) -> ChinaIncomeRepository:
        return self._income

    @property
    def cashReport(self) -> ChinaCashRepository:
        return self._cash


_repository = _MyRepository()


def set_local_path(local_path: str):
    _repository.refresh(local_path)


def load_by_stock_Id(stock_id: str, force_remote=False) -> pd.DataFrame:
    assert_report = _repository.assertReport.load_data(stock_id=stock_id, force_remote=force_remote)
    income_report = _repository.incomeReport.load_data(stock_id=stock_id, force_remote=force_remote)
    cash_report = _repository.cashReport.load_data(stock_id=stock_id, force_remote=force_remote)
    return pd.concat({"assert": assert_report, "income": income_report, "cash": cash_report})
