#  Copyright (c) 2020 RumorMill (https://chandlersong.me)
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
#

from setuptools import setup, find_packages

intall_requires = [
    'tushare>=0.6.8',
    'pandas-datareader>=0.2.1',
    'pandas>=0.18.1',
    'ta-lib>=0.4.0',
    'numpy>=1.11.0'
]


with open('README.md') as f:
    readme = f.read()

with open('LICENSE') as f:
    license = f.read()

setup(
    name='greenseer',
    packages =find_packages(exclude=['tests','tests.*','demo','demo.*']),
    version='0.1',
    license=license,
    author='Chandler Song',
    install_requires=intall_requires,
    author_email='chandler605@outlook.com',
    long_description=readme,
    description='just a toll to analysis stock market. a junior developer play'
)

"""
need freezegun for test
pip install freezegun
"""