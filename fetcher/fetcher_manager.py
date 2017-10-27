import sys
sys.path.append('../database_api')
import datetime
import database_lib
import csv
import io
import fetcher_lib
import logging
import random
import threading
import time


class UpdateStockDataJob(threading.Thread):
    def __init__(self, stock_list, thread_id, total_thread):
        threading.Thread.__init__(self)
        self.stock_list_ = stock_list
        self.thread_id_ = thread_id
        self.total_thread_ = total_thread
        self.db_client_ = database_lib.StockzDatabaseClient()

    def run(self):
        for symbol in self.stock_list_:
            if hash(symbol) % self.total_thread_ == self.thread_id_:
                last_update_date = self.db_client_.ReadLatestDateBySymbol(
                    symbol)
                health = self.db_client_.ReadStockHealth(symbol)
                if random.randint(0, 100) > health:
                    continue
                if not last_update_date:
                    last_update_date = datetime.date(2000, 1, 1)
                if last_update_date < datetime.date.today():
                    stock_fetcher = fetcher_lib.StockDataFetcher(
                        symbol, last_update_date)
                    time.sleep(0.5)
                    stock_data = stock_fetcher.UpdateStockData()
                    if stock_data is None:
                        self.db_client_.UpdateStockHealth(
                            symbol, max(4, health - 2))
                        continue
                    self.db_client_.UpdateStockHealth(symbol, 100)
                    logging.info('Shard %d updated stock: %s' %
                                 (self.thread_id_, symbol))
                    print('Shard %d updated stock: %s' %
                          (self.thread_id_, symbol))
                    reader = csv.DictReader(io.StringIO(str(stock_data)))
                    for data in reader:
                        if datetime.datetime.strptime(
                                data['date'], '%Y-%m-%d').date() > last_update_date:
                            self.db_client_.InsertStockData(
                                symbol, data['date'], open_p=data['open'], close_p=data['close'],
                                high_p=data['high'], low_p=data['low'], volume_p=data['volume'],
                                open_adj=data['adj_open'], close_adj=data['adj_close'],
                                high_adj=data['adj_high'], low_adj=data['adj_low'], volume_adj=data['adj_volume'],
                                ex_dividend=data['ex-dividend'], split_ratio=data['split_ratio'])


class FetcherManager:
    def __init__(self):
        self.company_list_ = []

    def UpdateCompanyList(self):
        db_client = database_lib.StockzDatabaseClient()
        logging.info('Start update NYSE')
        fetcher_nyse = fetcher_lib.CompanyListFetcher("NYSE")
        company_list = fetcher_nyse.GetList()
        logging.info('NYSE company list done\n')
        company_list.pop(0)
        for company in company_list:
            db_client.UpdateStockList(
                company[0], [company[1], 0, company[6], company[7]])
        logging.info('Start update NASDAQ')
        fetcher_nasdaq = fetcher_lib.CompanyListFetcher("NASDAQ")
        company_list = fetcher_nasdaq.GetList()
        logging.info('Nasdaq company list done\n')
        company_list.pop(0)
        for company in company_list:
            db_client.UpdateStockList(
                company[0], [company[1], 0, company[6], company[7]])

    def UpdateStockData(self, total_shard=1):
        db_client = database_lib.StockzDatabaseClient()
        self.job_shard_ = total_shard
        self.company_list_ = db_client.ReadStockList()
        job_list = []
        for i in range(0, self.job_shard_):
            job_list.append(UpdateStockDataJob(
                self.company_list_, i, self.job_shard_))
            job_list[i].start()
        logging.info("All stock updating shard started")
        for i in range(0, self.job_shard_):
            job_list[i].join()
        logging.info("All stock updating shard completed")
