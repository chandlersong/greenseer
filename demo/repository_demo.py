import unittest
from logging.config import fileConfig
from unittest import TestCase

from greenseer.repository.china_stock import create_daily_price_repository, BasicInfoRepository

fileConfig('logging_config.ini')


class TestChinaStock(TestCase):

    def setUp(self):
        self.__repository = create_daily_price_repository()

    def test_load_one_stock(self):
        print(self.__repository.load_data("600096").tail(1))


    def test_load_one_stock(self):
        repository = BasicInfoRepository()

        print(repository.load_data("600096"))
        print(repository.all_stock_basic_info.head(10))


if __name__ == '__main__':
    unittest.main()
