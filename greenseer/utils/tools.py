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

import matplotlib.pyplot as plt
import pandas as pd

_root_dir = "."
_image_root = os.path.join(_root_dir, "images")
_csv_root = os.path.join(_root_dir, "csv")


def refresh_report_data(root: str):
    global _root_dir
    global _image_root
    global _csv_root
    _root_dir = "./analysisData/" + root
    _image_root = os.path.join(_root_dir, "images")
    _csv_root = os.path.join(_root_dir, "csv")
    Path(_image_root).mkdir(parents=True, exist_ok=True)
    Path(_csv_root).mkdir(parents=True, exist_ok=True)


def save_fig(fig_id, tight_layout=True, fig_extension="png", resolution=800):
    Path(_image_root).mkdir(parents=True, exist_ok=True)
    path = os.path.join(_image_root, fig_id + "." + fig_extension)
    print("Saving figure", fig_id)
    if tight_layout:
        plt.tight_layout()
    plt.savefig(path, format=fig_extension, dpi=resolution)


def save_csv(data: pd.DataFrame, name: str):
    Path(_csv_root).mkdir(parents=True, exist_ok=True)
    path = os.path.join(_csv_root, name + "." + "csv")
    print("Saving scv:", path)
    data.to_csv(path, encoding='utf-8-sig')
