import unittest
from logging.config import fileConfig
from unittest import TestCase

from greenseer.repository.china_stock import create_daily_price_repository, get_gobal_basic_info_repository, \
    create_china_assert_repository

fileConfig('logging_config.ini')


class TestChinaStock(TestCase):

    def setUp(self):
        self.__repository = create_daily_price_repository()

    def test_load_one_stock(self):
        print(self.__repository.load_data("600096").tail(1))

    def test_load_basic_info(self):
        repository = get_gobal_basic_info_repository()

        print(repository.load_data("600096"))
        print(repository.all_stock_basic_info.head(5))

        repository = get_gobal_basic_info_repository()
        print(repository.load_data("600096"))

    def test_china_assert_repository(self):
        repository = create_china_assert_repository()
        print(repository.load_data("600096"))


if __name__ == '__main__':
    unittest.main()
