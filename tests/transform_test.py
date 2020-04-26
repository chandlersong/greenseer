import unittest
from logging.config import fileConfig

import pandas as pd
from pandas.testing import assert_frame_equal
from sklearn.preprocessing import FunctionTransformer

from greenseer.preprocessing.transformers import regular_expression_index_filter


class RegularExpressionIndexFilterTest(unittest.TestCase):
    def test_basic_case(self):
        data = pd.DataFrame({"x": [1, 2, 3], "y": [4, 5, 6]}, index=["a-bb", "c-aa", "a-cc"])

        transformer = FunctionTransformer(func=regular_expression_index_filter, validate=False,
                                          kw_args={"pattern": r'a-\w*'})

        expected = pd.DataFrame({"x": [1, 3], "y": [4, 6]}, index=["a-bb", "a-cc"])

        assert_frame_equal(expected, transformer.transform(data))
        self.assertEqual(True, True)


if __name__ == '__main__':
    unittest.main()
