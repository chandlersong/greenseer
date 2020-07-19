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
import multiprocessing

import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.model_selection import GridSearchCV
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler, PolynomialFeatures
from sklearn.svm import SVC

from greenseer.dataset.china_dataset import CODE_INDEX_NAME
from greenseer.preprocessing.transformers import MeanDistanceTransformer, to_numpy_type, join_data_frame

cash_subject_name = 'cash'
st_debt_name = 'short-term debt'
interest_name = 'interest'


def create_cash_debt_data_prepare_pipeline(industry_group: pd.DataFrame) -> Pipeline:
    distance_calculate = MeanDistanceTransformer(group_by="group",
                                                 columns=[cash_subject_name, st_debt_name])
    return Pipeline([
        ("append group", join_data_frame(join=industry_group, on="industry", new_index=[CODE_INDEX_NAME])),
        ("distance", distance_calculate),
        ('convert to numpy', to_numpy_type(columns=[cash_subject_name, st_debt_name, "distance"]))
    ]
    )


def create_cash_debt_relative_search(data_debt_group: pd.DataFrame, industry_mean: pd.DataFrame) -> GridSearchCV:
    # clf = OneVsRestClassifier(estimator=SVC(decision_function_shape="ovo"), n_jobs=multiprocessing.cpu_count())
    clf = SVC()
    C = np.arange(0, 101, 10)
    C[0] = 1
    grammar = np.power(10., np.arange(-3, 3))
    company_cluster = KMeans()
    param_grid = [
        {
            'model__kernel': ["linear"],
            'model__C': C,
            'poly_features__degree': np.arange(1, 3),
            'cluster__n_clusters': np.arange(2, 6)
        },
        {
            'model__kernel': ["rbf"],
            'model__C': C,
            'model__gamma': grammar,
            'poly_features__degree': np.arange(1, 3),
            'cluster__n_clusters': np.arange(2, 6)
        },
    ]

    debt_by_group_pipeline = Pipeline([
        ('poly_features', PolynomialFeatures()),
        ("standard", StandardScaler()),
        ('cluster', company_cluster),
        ("model", clf)
    ])

    return GridSearchCV(debt_by_group_pipeline, param_grid, cv=3,
                        scoring='f1_micro',
                        return_train_score=True,
                        n_jobs=multiprocessing.cpu_count())
