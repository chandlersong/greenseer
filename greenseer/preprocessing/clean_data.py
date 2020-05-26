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
from typing import List

import numpy as np
import pandas as pd
from sklearn.base import TransformerMixin, BaseEstimator

from greenseer.utils.annotation import FunctionTransformerWrapper


@FunctionTransformerWrapper()
def remove_inf_and_na(X: pd.DataFrame = None) -> pd.DataFrame:
    X.replace([np.inf, -np.inf], np.nan)
    return X.dropna()


class RemoveAbnormalFilter(BaseEstimator, TransformerMixin):

    def __init__(self, column: List[str], quantile=0.999, mode="both"):
        """

        :param column: column name
        :param quantile:  quantile of range
        :param mode: both: exclude both side, the range will be (1-quantile)/2 to 1- (1-quantile)/2
                     high: exclude high side, the range will be 0 to quantile
                     low: exclude low side, the range will quantile to 100
        """
        self._columns = column
        self._quantile = quantile
        self._mode = mode
        self._low = None
        self._high = None

    def fit(self, X: pd.DataFrame, y=None):

        if self._mode == "both":
            boundary = (1 - self._quantile) / 2
            self._high = X[self._columns].quantile(1 - boundary)
            self._low = X[self._columns].quantile(boundary)
        elif self._mode == "high":
            self._high = X[self._columns].quantile(self._quantile)
            self._low = X[self._columns].quantile(0)
        elif self._mode == "low":
            self._high = X[self._columns].quantile(1)
            self._low = X[self._columns].quantile(1 - self._quantile)
        return self

    def transform(self, X: pd.DataFrame):
        for column in self._columns:
            X = X[(self._low[column] <= X[column]) & (X[column] <= self._high[column])]
        return X
