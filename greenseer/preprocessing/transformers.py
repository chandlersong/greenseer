import logging
import re
from functools import partial

import pandas as pd

_logger = logging.getLogger("transformer")


def regular_expression_index_filter(X: pd.DataFrame, pattern, level=None) -> pd.DataFrame:
    drop_indexes = [index for index in X.index.get_level_values(level) if not re.match(pattern, index)]
    _logger.info("remove {} rows,details is {}".format(len(drop_indexes), drop_indexes))
    return X.drop(drop_indexes, level=level)


def regular_expression_column_filter(X: pd.DataFrame, patterns, level=None) -> pd.DataFrame:
    columns = set(X.columns.get_level_values(level))
    for pattern in patterns:
        keep = [column for column in columns if re.match(pattern, column)]
        columns = columns.difference(keep)

    _logger.info("remove {} rows,details is {}".format(len(columns), columns))
    return X.drop(columns, axis=1, level=level)


pick_annual_report_china = partial(regular_expression_index_filter, pattern=r'\d{4}-12-31', level=1)
