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
from functools import partial

import numpy as np
import pandas as pd

from greenseer.repository.china_stock import create_china_stock_assert_repository, create_china_stock_income_repository, \
    create_china_stock_cash_repository, ChinaAssertRepository, ChinaIncomeRepository, ChinaCashRepository

ASSERT_REPORT = "assert"

INCOME_REPORT = "income"

CASH_REPORT = "cash"

DEFAULT_LOCAL_PATH = "reportData"


class ChinaReportRepository:
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


_repository = ChinaReportRepository()


def set_local_path(local_path: str):
    _repository.refresh(local_path)


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
    assert_report = repository.assertReport.load_data(stock_id=stock_id, force_remote=force_remote,
                                                      remote_delay_max_seconds=max_sleep_seconds)
    income_report = repository.incomeReport.load_data(stock_id=stock_id, force_remote=force_remote,
                                                      remote_delay_max_seconds=max_sleep_seconds)
    cash_report = repository.cashReport.load_data(stock_id=stock_id, force_remote=force_remote,
                                                  remote_delay_max_seconds=max_sleep_seconds)
    return pd.concat({stock_id: pd.concat([assert_report, income_report, cash_report], axis=1)})


fetch_one_report = partial(load_by_stock_id, repository=_repository)
fetch_multi_report = partial(load_multi_data, repository=_repository)
