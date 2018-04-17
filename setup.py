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

