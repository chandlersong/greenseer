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

        expected = pd.DataFrame({"x": [3, 1], "y": [6, 4]}, index=["a-cc", "a-bb"])

        assert_frame_equal(expected, transformer.transform(data))
        self.assertEqual(True, True)

    def test_pick_annual_report_china(self):
        data = pd.DataFrame({"x": [1, 2, 3, 4, 5], "y": [6, 7, 8, 9, 0]},
                            index=(pd.MultiIndex.from_tuples(
                                [("a", "2020-12-31"), ("a", "2020-09-31"), ("a", "2019-12-31"), ("b", "2020-12-31"),
                                 ("b", "2020-09-31")])))

        transformer = FunctionTransformer(func=pick_annual_report_china, validate=False)

        expected = pd.DataFrame({"x": [4, 1, 3], "y": [9, 6, 8]},
                                index=(pd.MultiIndex.from_tuples(
                                    [("b", "2020-12-31"), ("a", "2020-12-31"), ("a", "2019-12-31")])))
        transform = transformer.transform(data)
        print(transform)
        assert_frame_equal(expected, transform)
        self.assertEqual(True, True)


class RegularExpressionColumnFilterTest(unittest.TestCase):
    def test_basic_case(self):
        data = pd.DataFrame({"我爱钱": [1, 2, 3], "我喜欢古董": [4, 5, 6], "我恨没钱": [7, 8, 9]}, index=["a", "b", "c"])

        transformer = FunctionTransformer(func=regular_expression_column_filter, validate=False,
                                          kw_args={"patterns": [r'我爱', r'我喜欢']})

        expected = pd.DataFrame({"我爱钱": [3, 2, 1], "我喜欢古董": [6, 5, 4]}, index=["c","b", "a"])

        assert_frame_equal(expected, transformer.transform(data))
        self.assertEqual(True, True)


if __name__ == '__main__':
    unittest.main()
