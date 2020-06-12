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


@dataclass
class DistributeGramEntry:
    x: List
    y: List
    fmt: str
    label: str


def plot_distribute_gram(plt, data: List[DistributeGramEntry], labels: List[str]):
    for entry in data:
        plt.plot(entry.x, entry.y, entry.fmt, label=entry.label)
    plt.xlabel(labels[0], fontsize=15)
    plt.ylabel(labels[1], fontsize=15)
    plt.legend(loc="upper left")
