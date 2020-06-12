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
from dataclasses import dataclass
from typing import List

import numpy as np
from matplotlib.pylab import mpl, FuncFormatter


@dataclass
class ViolinInfos:
    labels: List[str]
    color: List[str] = None


def plot_violin_plot_with_color(ax, data, title: str, info: ViolinInfos, y_to_percent=False):
    parts = ax.violinplot(data)
    _set_axis_style(ax, title, info.labels, y_to_percent)
    if info.color is None:
        info.color = list()
        length = len(info.labels)
        for i in range(1, length + 1):
            info.color.append(mpl.cm.Spectral(i / length))

    for part, color in zip(parts['bodies'], info.color):
        part.set_facecolor(color)
        part.set_edgecolor('black')


def to_percent(temp, position):
    return '%1.0f' % (100 * temp) + '%'


def _set_axis_style(ax, labelName, labels, y_to_percent):
    ax.get_xaxis().set_tick_params(direction='out')
    ax.xaxis.set_ticks_position('bottom')
    ax.set_xticks(np.arange(1, len(labels) + 1))
    ax.set_xticklabels(labels)
    ax.set_xlim(0.25, len(labels) + 0.75)
    ax.set_xlabel(labelName)
    if y_to_percent:
        ax.yaxis.set_major_formatter(FuncFormatter(to_percent))
