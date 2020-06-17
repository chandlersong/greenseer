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

import logging
import re
from functools import partial
from typing import List

import pandas as pd

from greenseer.dataset.china_dataset import stock_info, CODE_INDEX_NAME, RELEASE_AT_INDEX_NAME
from greenseer.utils.annotation import FunctionTransformerWrapper

_logger = logging.getLogger()


@FunctionTransformerWrapper()
def append_industry_transform(X: pd.DataFrame = None) -> pd.DataFrame:
    industry_category = stock_info()[["industry"]]
    data_with_industry = pd.merge(X.reset_index(), industry_category.reset_index(), on=CODE_INDEX_NAME,
                                  how='left')
    data = data_with_industry.set_index([CODE_INDEX_NAME, RELEASE_AT_INDEX_NAME])
    data.index.set_names(["code", 'release_at'], inplace=True)
    return data


@FunctionTransformerWrapper()
def regular_expression_index_filter(X: pd.DataFrame, pattern, level=None) -> pd.DataFrame:
    drop_indexes = [index for index in X.index.get_level_values(level) if not re.match(pattern, index)]
    _logger.info("remove {} rows,details is {}".format(len(drop_indexes), drop_indexes))
    return X.drop(drop_indexes, level=level).sort_index(level=level, ascending=False)


@FunctionTransformerWrapper()
def regular_expression_column_filter(X: pd.DataFrame, patterns, level=None, rename=None) -> pd.DataFrame:
    columns = set(X.columns.get_level_values(level))
    for pattern in patterns:
        keep = [column for column in columns if re.match(pattern, column)]
        columns = columns.difference(keep)

    _logger.info("remove {} rows,details is {}".format(len(columns), columns))
    result = X.drop(columns, axis=1, level=level).sort_index(level=level, ascending=False)

    if rename is not None:
        result.rename(rename, inplace=True, axis=1)

    return result


@FunctionTransformerWrapper()
def sum_column_transform(X: pd.DataFrame, new_name: str, columns: List[str], level=None) -> pd.DataFrame:
    X[new_name] = X[columns].sum(level=level, axis=1)
    return X


@FunctionTransformerWrapper()
def re_sum_column_transform(X: pd.DataFrame, new_name: str, patterns, level=None) -> pd.DataFrame:
    columns = _match_columns_by_patterns(set(X.columns.get_level_values(level)), patterns)
    X[new_name] = X[columns].sum(level=level, axis=1)
    return X


@FunctionTransformerWrapper()
def re_percent_column_transform(X: pd.DataFrame, new_name: str, numerator: List[str], denominator: List[str],
                                level=None) -> pd.DataFrame:
    headers = set(X.columns.get_level_values(level))
    n = _match_columns_by_patterns(headers, numerator)
    d = _match_columns_by_patterns(headers, denominator)
    X[new_name] = X[n].sum(level=level, axis=1) / X[d].sum(level=level, axis=1)
    return X


def _match_columns_by_patterns(headers, numerator):
    n = set()
    for pattern in numerator:
        n.update([column for column in headers if re.match(pattern, column)])
    return n


@FunctionTransformerWrapper()
def percent_column_transform(X: pd.DataFrame, new_name: str, numerator: List[str], denominator: List[str],
                             level=None) -> pd.DataFrame:
    """

    :param X:
    :param new_name:
    :param numerator: 分子
    :param denominator: 分母
    :param level:
    :return:
    """
    X[new_name] = X[numerator].sum(level=level, axis=1) / X[denominator].sum(level=level, axis=1)
    return X


@FunctionTransformerWrapper()
def pick_row_by_index_month(X: pd.DataFrame, month: int, level=None) -> pd.DataFrame:
    return X.loc[X.index.get_level_values(level).month == month, :]


pick_annual_report_china = partial(pick_row_by_index_month, month=12, level=1)
