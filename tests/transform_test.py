import unittest

import pandas as pd
from pandas.testing import assert_frame_equal
from sklearn.preprocessing import FunctionTransformer

from greenseer.preprocessing.transformers import regular_expression_index_filter, pick_annual_report_china, \
    regular_expression_column_filter


class RegularExpressionIndexFilterTest(unittest.TestCase):
    def test_basic_case(self):
        data = pd.DataFrame({"x": [1, 2, 3], "y": [4, 5, 6]}, index=["a-bb", "c-aa", "a-cc"])

        transformer = FunctionTransformer(func=regular_expression_index_filter, validate=False,
                                          kw_args={"pattern": r'a-\w*'})

        expected = pd.DataFrame({"x": [1, 3], "y": [4, 6]}, index=["a-bb", "a-cc"])

        assert_frame_equal(expected, transformer.transform(data))
        self.assertEqual(True, True)

    def test_pick_annual_report_china(self):
        data = pd.DataFrame({"x": [1, 2, 3, 4, 5], "y": [6, 7, 8, 9, 0]},
                            index=(pd.MultiIndex.from_tuples(
                                [("a", "2020-12-30"), ("a", "2020-09-30"), ("a", "2019-12-30"), ("b", "2020-12-30"),
                                 ("b", "2020-09-30")])))

        transformer = FunctionTransformer(func=pick_annual_report_china, validate=False)

        expected = pd.DataFrame({"x": [1, 3, 4], "y": [6, 8, 9]},
                                index=(pd.MultiIndex.from_tuples(
                                    [("a", "2020-12-30"), ("a", "2019-12-30"), ("b", "2020-12-30")])))
        transform = transformer.transform(data)
        print(transform)
        assert_frame_equal(expected, transform)
        self.assertEqual(True, True)


class RegularExpressionColumnFilterTest(unittest.TestCase):
    def test_basic_case(self):
        data = pd.DataFrame({"我爱钱": [1, 2, 3], "我喜欢古董": [4, 5, 6], "我恨没钱": [7, 8, 9]}, index=["a", "b", "c"])

        transformer = FunctionTransformer(func=regular_expression_column_filter, validate=False,
                                          kw_args={"patterns": [r'我爱', r'我喜欢']})

        expected = pd.DataFrame({"我爱钱": [1, 2, 3], "我喜欢古董": [4, 5, 6]}, index=["a", "b", "c"])

        assert_frame_equal(expected, transformer.transform(data))
        self.assertEqual(True, True)


if __name__ == '__main__':
    unittest.main()
