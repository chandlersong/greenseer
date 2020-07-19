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
from sklearn.metrics import accuracy_score, recall_score, f1_score


def compose_classify_score(y, y_pred, average: str = "micro") -> str:
    accuracy = accuracy_score(y, y_pred)
    recall = recall_score(y, y_pred, average=average)
    f1 = f1_score(y, y_pred, average=average)
    return "accuracy {},recall is {},f1 is {}".format(accuracy, recall, f1)
