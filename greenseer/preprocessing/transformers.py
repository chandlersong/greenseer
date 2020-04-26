import logging
import re
from functools import partial

import pandas as pd

_logger = logging.getLogger("transformer")


def regular_expression_index_filter(X: pd.DataFrame, pattern, level=None) -> pd.DataFrame:
    drop_indexes = [index for index in X.index.get_level_values(level) if not re.match(pattern, index)]
    _logger.info("remove {} rows,details is {}".format(len(drop_indexes), drop_indexes))
    return X.drop(drop_indexes, level=level)


pick_annual_report_china = partial(regular_expression_index_filter, pattern=r'\d{4}-12-30', level=1)
