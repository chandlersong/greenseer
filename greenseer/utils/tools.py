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
import os
from pathlib import Path

import joblib
import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.pylab import mpl
from sklearn.base import BaseEstimator

_root_dir = "."


def enable_matplotlib_chinese():
    plt.rcParams['font.family'] = 'Source Han Serif SC'
    mpl.rcParams['font.sans-serif'] = ['Source Han Serif SC']  # 指定默认字体
    mpl.rcParams['axes.unicode_minus'] = False  # 解决保存图像是负号'-'显示为方块的问题


class DataSaver:
    def __init__(self, root: str = _root_dir):
        self._root_dir = None
        self._image_root = None
        self._csv_root = None
        self._sklearn_root = None
        self.refresh_report_data(root)

    def refresh_report_data(self, root: str):
        self._root_dir = "./analysisData/" + root
        self._image_root = os.path.join(self._root_dir, "images")
        self._csv_root = os.path.join(self._root_dir, "csv")
        self._sklearn_root = os.path.join(self._root_dir, "sklearn")
        Path(self._image_root).mkdir(parents=True, exist_ok=True)
        Path(self._csv_root).mkdir(parents=True, exist_ok=True)
        Path(self._sklearn_root).mkdir(parents=True, exist_ok=True)

    def save_fig(self, fig_id, tight_layout=True, fig_extension="png", resolution=800):
        path = os.path.join(self._image_root, fig_id + "." + fig_extension)
        print("Saving figure", fig_id)
        if tight_layout:
            plt.tight_layout()
        plt.savefig(path, format=fig_extension, dpi=resolution)

    def save_csv(self, data: pd.DataFrame, name: str):
        path = os.path.join(self._csv_root, name + "." + "csv")
        print("Saving scv:", path)
        data.to_csv(path, encoding='utf-8-sig')

    def save_sklearn_model(self, model, name: str):
        path = os.path.join(self._sklearn_root, name + "." + "pkl")
        print("Saving sklearn model:", path)
        joblib.dump(model, path)

    def load_sklearn_model(self, name: str) -> BaseEstimator:
        """
        load model file
        :param name: model file name
        :return: None if not exist
        """
        path = os.path.join(self._sklearn_root, name + "." + "pkl")

        if not os.path.exists(path):
            return None
        return joblib.load(path)
