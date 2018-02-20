from setuptools import setup


intall_requires = [
    'tushare>=0.6.8',    
    'pandas-datareader>=0.2.1',
    'pandas>=0.18.1',
    'ta-lib>=0.4.0',
    'numpy>=1.11.0'
]


setup(
    name='greenseer',
    packages=['greenseer'],
    package_dir = {'greenseer': 'src'},
    version='0.1',
    license='apache2',
    author='Chandler Song',
    install_requires=intall_requires,
    author_email='chandler605@outlook.com',
    description='just a toll to analysis stock market. a junior developer play'
)
