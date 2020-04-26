import logging

import pandas as pd
import re

_logger = logging.getLogger()


def regular_expression_index_filter(X: pd.DataFrame, pattern) -> pd.DataFrame:
    drop_indexes = [index for index in X.index if not re.match(pattern, index)]
    _logger.info("remove {} rows,details is {}".format(len(drop_indexes), drop_indexes))
    return X.drop(drop_indexes).sort_index(axis=0)
