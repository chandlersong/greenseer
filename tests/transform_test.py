# Copyright (c) 2020 GreenSeer (https://chandlersong.me)
# Copyright (c) 2020 chandler.song
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

import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal

from greenseer.dataset.china_dataset import CODE_INDEX_NAME, RELEASE_AT_INDEX_NAME
from greenseer.preprocessing.clean_data import RemoveAbnormalFilter
from greenseer.preprocessing.transformers import regular_expression_index_filter, pick_annual_report_china, \
    regular_expression_column_filter, sum_column_transform, percent_column_transform, re_sum_column_transform, \
    re_percent_column_transform, pick_row_by_index_month, unstack_release_at, MeanDistanceTransformer


class TimeSeriesFilterTest(unittest.TestCase):

    def test_pick_row_by_index_month(self):
        data = pd.DataFrame({"x": [1, 2, 3, 4, 5], "y": [6, 7, 8, 9, 0]},
                            index=[pd.Timestamp("2020-12-31"), pd.Timestamp("2020-09-30"),
                                   pd.Timestamp("2019-12-31"),
                                   pd.Timestamp("2020-12-31"),
                                   pd.Timestamp("2020-09-30")])

        transformer = pick_row_by_index_month(month=9)

        expected = pd.DataFrame({"x": [2, 5], "y": [7, 0]},
                                index=[pd.Timestamp("2020-09-30"), pd.Timestamp("2020-09-30")])
        actual = transformer.transform(data)
        print(actual)
        assert_frame_equal(expected, actual)
        self.assertEqual(True, True)

    def test_pick_annual_report_china(self):
        data = pd.DataFrame({"x": [1, 2, 3, 4, 5], "y": [6, 7, 8, 9, 0]},
                            index=(pd.MultiIndex.from_tuples(
                                [("a", pd.Timestamp("2020-12-31")), ("a", pd.Timestamp("2020-09-30")),
                                 ("a", pd.Timestamp("2019-12-31")),
                                 ("b", pd.Timestamp("2020-12-31")),
                                 ("b", pd.Timestamp("2020-09-30"))])))

        transformer = pick_annual_report_china()

        expected = pd.DataFrame({"x": [1, 3, 4], "y": [6, 8, 9]},
                                index=(pd.MultiIndex.from_tuples(
                                    [("a", pd.Timestamp("2020-12-31")), ("a", pd.Timestamp("2019-12-31")),
                                     ("b", pd.Timestamp("2020-12-31"))])))
        actual = transformer.transform(data)
        print(actual)
        assert_frame_equal(expected, actual)
        self.assertEqual(True, True)

    def test_hello_world(self):
        from sklearn.datasets import fetch_openml
        mnist = fetch_openml('mnist_784', version=1, cache=True)

        X = mnist["data"]
        y = mnist["target"].astype(np.uint8)

        X_train = X[:60000]
        y_train = y[:60000]
        X_test = X[60000:]
        y_test = y[60000:]
        print(y_train)

class RegularExpressionIndexFilterTest(unittest.TestCase):
    def test_basic_case(self):
        data = pd.DataFrame({"x": [1, 2, 3], "y": [4, 5, 6]}, index=["a-bb", "c-aa", "a-cc"])

        transformer = regular_expression_index_filter(pattern=r'a-\w*')

        expected = pd.DataFrame({"x": [3, 1], "y": [6, 4]}, index=["a-cc", "a-bb"])

        assert_frame_equal(expected, transformer.transform(data))
        self.assertEqual(True, True)


class RegularExpressionColumnFilterTest(unittest.TestCase):
    def test_basic_case(self):
        data = pd.DataFrame({"我爱钱": [1, 2, 3], "我喜欢古董": [4, 5, 6], "我恨没钱": [7, 8, 9]}, index=["a", "b", "c"])

        transformer = regular_expression_column_filter(patterns=[r'我爱', r'我喜欢'])

        expected = pd.DataFrame({"我爱钱": [3, 2, 1], "我喜欢古董": [6, 5, 4]}, index=["c", "b", "a"])

        assert_frame_equal(expected, transformer.transform(data))
        self.assertEqual(True, True)


class ReExpressionColumnSumTest(unittest.TestCase):
    def test_basic_case(self):
        data = pd.DataFrame({"我爱钱": [1, 2, 3], "我喜欢古董": [4, 5, 6], "我恨没钱": [7, 8, 9]}, index=["a", "b", "c"])

        transformer = re_sum_column_transform(new_name="new", patterns=[r'我爱', r'我喜欢'])

        expected = pd.DataFrame({"我爱钱": [1, 2, 3], "我喜欢古董": [4, 5, 6], "我恨没钱": [7, 8, 9], "new": [5, 7, 9]},
                                index=["a", "b", "c"])
        assert_frame_equal(expected, transformer.transform(data))
        self.assertEqual(True, True)


class ReExpressionColumnPercentTest(unittest.TestCase):
    def test_basic_case(self):
        data = pd.DataFrame({"我爱钱": [1, 2, 3], "我喜欢古董": [4, 5, 6], "我恨没钱": [7, 8, 9]}, index=["a", "b", "c"])

        transformer = re_percent_column_transform(new_name="new", numerator=[r'我爱', r'我喜欢'], denominator=[r'我恨'])

        expected = pd.DataFrame({"我爱钱": [1, 2, 3], "我喜欢古董": [4, 5, 6], "我恨没钱": [7, 8, 9], "new": [5 / 7, 7 / 8, 9 / 9]},
                                index=["a", "b", "c"])

        assert_frame_equal(expected, transformer.transform(data))
        self.assertEqual(True, True)


class SumColumnTransformerTest(unittest.TestCase):
    def test_basic_case(self):
        data = pd.DataFrame({"x": [1, 2, 3], "y": [4, 5, 6], "z": [7, 8, 9]}, index=["a", "b", "c"])

        transform = sum_column_transform(new_name="new", columns=["x", "y"])

        expected = pd.DataFrame({"x": [1, 2, 3], "y": [4, 5, 6], "z": [7, 8, 9], "new": [5, 7, 9]},
                                index=["a", "b", "c"])

        actual = transform.transform(data)
        print(actual)
        assert_frame_equal(expected, actual)
        self.assertTrue(True)


class PercentColumnTransformerTest(unittest.TestCase):
    def test_basic_case(self):
        data = pd.DataFrame({"x": [1, 2, 3], "y": [4, 5, 6], "z": [7, 8, 9]}, index=["a", "b", "c"])

        transform = percent_column_transform(new_name="new", numerator=["x", "y"], denominator=["z"])

        expected = pd.DataFrame({"x": [1, 2, 3], "y": [4, 5, 6], "z": [7, 8, 9], "new": [5 / 7, 7 / 8, 9 / 9]},
                                index=["a", "b", "c"])

        actual = transform.transform(data)
        print(actual)
        assert_frame_equal(expected, actual)
        self.assertTrue(True)


class UnstackReleaseAtTest(unittest.TestCase):
    def test_unstack_drop_if_missing(self):
        index = pd.MultiIndex.from_product(
            [["a", "b"], pd.date_range("20200622", periods=5, freq="Q")],
            names=[CODE_INDEX_NAME, RELEASE_AT_INDEX_NAME])
        index = index.append(pd.MultiIndex.from_product([["c"], pd.date_range("20191201", periods=5, freq="Q")]))
        data = pd.DataFrame({"x": np.arange(0, 15), "y": np.random.random(15)}, index=index)
        expected = pd.DataFrame(
            [np.arange(0, 5), np.arange(5, 10)],
            columns=pd.date_range("20200622", periods=5, freq="Q"),
            index=["a", "b"]
        )
        expected.columns.names = [RELEASE_AT_INDEX_NAME]
        transform = unstack_release_at(start="2020-06-30", end="2021-06-30", column_name="x")
        actual = transform.transform(data)
        assert_frame_equal(expected, actual, check_dtype=False)


class CleanDataTest(unittest.TestCase):
    def test_remove_abnormal_filter_both(self):
        origin = np.random.random([100, 3])
        values = np.vstack([origin, [0.5, 100, 0.5], [0.5, 0.5, -1]])
        test_filter = RemoveAbnormalFilter(["y", "z"])
        self.remove_abnormal_and_check(test_filter, values, shape=(100, 3))

    def test_remove_abnormal_filter_high(self):
        origin = np.random.random([100, 3])
        values = np.vstack([origin, [0.5, 100, 0.5], [0.5, 0.5, 100]])
        test_filter = RemoveAbnormalFilter(["y", "z"], mode='high')
        self.remove_abnormal_and_check(test_filter, values)

    def test_remove_abnormal_filter_low(self):
        origin = np.random.random([100, 3])
        values = np.vstack([origin, [0.5, -1, 0.5], [0.5, -2, 0.5]])
        test_filter = RemoveAbnormalFilter(["y", "z"], mode='low')

        self.remove_abnormal_and_check(test_filter, values)

    def remove_abnormal_and_check(self, test_filter, values, shape=(100, 3)):
        data = pd.DataFrame({"x": values[:, 0], "y": values[:, 1], "z": values[:, 2]}, index=list(np.arange(0, 102)))
        actual = test_filter.fit_transform(data)
        self.assertEqual(shape, actual.shape)


class MeanDistanceTransformerTest(unittest.TestCase):

    def test_fit_transform(self):
        data = pd.DataFrame({"x": [1, 2, 3], "y": [5, 5, 6], "z": ["a", "a", "b"]}, index=["a", "b", "c"])
        mean = data.groupby("z").mean()
        expected = data.copy()
        expected["distance"] = expected.apply(calculate_distance(mean), axis=1)

        transformer = MeanDistanceTransformer(group_by="z", columns=["x", "y"])
        actual = transformer.fit_transform(data)
        assert_frame_equal(expected, actual, check_dtype=False)


def calculate_distance(mean: pd.DataFrame):
    def calculate(row: pd.Series):
        center_ids = mean.loc[row["z"]][["x", "y"]].values
        val = row[["x", "y"]].values
        return np.linalg.norm(center_ids - val)

    return calculate


if __name__ == '__main__':
    unittest.main()
