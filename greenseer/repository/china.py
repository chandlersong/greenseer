import logging

import numpy as np
from greenseer.repository import BaseRepository
from pandas import DataFrame

TU_SHARE_SINA_DAILY = {'amount': np.float64, 'volume': np.float64}


class DailyPriceRepository(BaseRepository):
    logger = logging.getLogger()

    def __init__(self, local_source, remote_source):
        BaseRepository.__init__(self, local_source, remote_source)

    def load_data(self, stock_id, force_local=False) -> DataFrame:

        local_data = self.local_source.load_data(stock_id)
        if local_data.empty or self.check_data_dirty(stock_id, local_data):
            self.logger.info("{} is dirty or empty, local data will be refresh".format(stock_id))
            remote_data = self.initial_remote_data(stock_id)
            self.save_or_update_local(stock_id, remote_data)
            return remote_data
        else:
            return local_data

    def initial_remote_data(self, stock_id) -> DataFrame:
        pass

    def save_or_update_local(self, stock_id, data: DataFrame):
        self.local_source.refresh_data(stock_id, data)

    def check_data_dirty(self, stock_id, local_data: DataFrame):
        latest_record = local_data.tail(1)

        if latest_record.empty:
            return True

        last_date = latest_record.index[0].strftime("%Y-%m-%d")
        remote_record = self.remote_source(stock_id, start=last_date, end=last_date)

        result = not latest_record.sort_index(axis=1).equals(remote_record.sort_index(axis=1))
        self.logger.info("{} local data is dirty".format(stock_id))
        return result
