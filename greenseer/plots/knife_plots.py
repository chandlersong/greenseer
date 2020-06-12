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
import numpy as np
from matplotlib.pylab import mpl
from matplotlib.ticker import FixedLocator, FixedFormatter


def plot_knife(plt, data, group_column='group', value_column_format="group{}_proba"):
    padding = len(data) // 30
    pos = padding
    ticks = []

    groups = data[group_column].unique()
    length = len(groups)
    for i in groups:
        color = mpl.cm.Spectral(i / length)
        column_name = value_column_format.format(i)
        values = data.loc[(data[group_column] == i)][column_name].sort_values().values
        plt.fill_betweenx(np.arange(pos, pos + len(values)), 0, values,
                          facecolor=color, edgecolor=color, alpha=0.7)
        ticks.append(pos + len(values) // 2)
        pos += len(values) + padding

    plt.gca().yaxis.set_major_locator(FixedLocator(ticks))
    plt.gca().yaxis.set_major_formatter(FixedFormatter(groups))
    plt.axvline(x=0.5, color="red", linestyle="--")
