import unittest
from unittest import TestCase

from greenseer.configuration import create_configuration


class TestConfiguration(TestCase):

    def test_auto_load(self):
        config = create_configuration()

        self.assertEqual('365', config.get_str("china_stock_config", "remote_fetch_days"))
        self.assertEqual(365, config.get_int_value("china_stock_config", "remote_fetch_days"))

    def test_manually_load(self):
        config = create_configuration("config/test_config.properties")

        self.assertEqual('366', config.get_str("china_stock_config", "remote_fetch_days"))
        self.assertEqual(366, config.get_int_value("china_stock_config", "remote_fetch_days"))


if __name__ == "__main__":
    if __name__ == '__main__':
        unittest.main()
