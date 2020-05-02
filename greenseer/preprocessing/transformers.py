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

import pandas as pd

_logger = logging.getLogger("transformer")


def regular_expression_index_filter(X: pd.DataFrame, pattern, level=None) -> pd.DataFrame:
    drop_indexes = [index for index in X.index.get_level_values(level) if not re.match(pattern, index)]
    _logger.info("remove {} rows,details is {}".format(len(drop_indexes), drop_indexes))
    return X.drop(drop_indexes, level=level).sort_index(level=level, ascending=False)


def regular_expression_column_filter(X: pd.DataFrame, patterns, level=None) -> pd.DataFrame:
    columns = set(X.columns.get_level_values(level))
    for pattern in patterns:
        keep = [column for column in columns if re.match(pattern, column)]
        columns = columns.difference(keep)

    _logger.info("remove {} rows,details is {}".format(len(columns), columns))
    return X.drop(columns, axis=1, level=level).sort_index(level=level, ascending=False)


pick_annual_report_china = partial(regular_expression_index_filter, pattern=r'\d{4}-12-31', level=1)
