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
from sklearn.preprocessing import FunctionTransformer


class FunctionTransformerWrapper:
    def __init__(self, validate=False):
        self._validate = validate

    def __call__(self, original_func):
        def wrap_function_transformer(*args, **kwargs):
            return FunctionTransformer(original_func, validate=self._validate, kw_args=kwargs)

        return wrap_function_transformer
