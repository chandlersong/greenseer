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
import csv
import os
import unittest

import pandas as pd
import pinyin

from greenseer.repository.china_stock import get_global_basic_info_repository

PREPARE_DATA = "prepare_data"

REPORT_TYPES = ["china_assert_reports", "china_cash_reports", "china_income_reports"]


def create_folder_if_not_exist(path: str):
    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)


class ChangeDataFormat(unittest.TestCase):
    def test_change_to_others(self):
        source_path = "allReportsData"
        # source_path = "reportData"

        export_path = PREPARE_DATA
        create_folder_if_not_exist(export_path)
        new_column_dict = {}
        for report_type in REPORT_TYPES:
            create_folder_if_not_exist(export_path + "/" + report_type)
            new_column_dict[report_type] = translate_columns(report_type)
        all_stock_info = get_global_basic_info_repository().load_data("xx")
        all_stock_info.to_parquet(export_path + "/stock_info.parquet")

        all_stock = all_stock_info.index
        # all_stock = ["600096"]
        for stock_id in all_stock:
            for report_type in REPORT_TYPES:
                transform_one_stock(stock_id, source_path, report_type, new_column_dict[report_type])


def transform_one_stock(stock_id: str, source_path: str, report_type: str, column_dict: dict) -> bool:
    source_file = "{}/greenseer/{}/{}.gz".format(source_path, report_type, stock_id)
    if not os.path.exists(source_file):
        print("report {} stock id {} not exist".format(report_type, stock_id))
        return False
    data = pd.read_csv(source_file, index_col=0,
                       compression="gzip",
                       parse_dates=True, dtype=None)
    export_data = data.T
    export_data.columns = [column_dict[name] for name in export_data.columns]
    export_data["code"] = stock_id
    export_data.to_parquet("{}/{}/{}.parquet".format(PREPARE_DATA, report_type, stock_id))
    return True


class TestPinyin(unittest.TestCase):

    def test_print_example(self):
        for report_type in REPORT_TYPES:
            translate_columns(report_type)


def translate_columns(report_type: str) -> dict:
    source_file = "{}/greenseer/{}/{}.gz".format("reportData", report_type, "600096")
    data = pd.read_csv(source_file, index_col=0,
                       compression="gzip",
                       parse_dates=True, dtype=None)
    values = {}
    duplicate = set()
    for name in data.index:
        initial = pinyin.get_initial(name[:-4]).replace(" ", "").replace("：", "").replace("、", "").replace("(",
                                                                                                           "").replace(
            ")", "")

        if initial in duplicate:
            print("{} is duplicate".format(initial))
            initial = get_not_duplicate_name(duplicate, initial)
        duplicate.add(initial)
        values[name] = initial
        print("name is {},initial is {}".format(name, initial))
    print("{},{}".format(len(values.keys()), len(duplicate)))
    write_to_local(values, "{}/{}.csv".format(PREPARE_DATA, report_type))
    return values


def get_not_duplicate_name(duplicate: set, name: str) -> str:
    new_name_format = "{}_{}"
    idx = 1
    while True:
        new_name = new_name_format.format(name, idx)
        if new_name not in duplicate:
            return new_name
        idx = idx + 1


def write_to_local(name_dict: dict, file_name: str):
    with open(file_name, 'w') as f:
        w = csv.DictWriter(f, ["name", "initial"])
        w.writeheader()
        for name in name_dict:
            w.writerow({"name": name, "initial": name_dict[name]})


if __name__ == '__main__':
    unittest.main()
