import sys
sys.path.append('../database_api')
import datetime
import database_lib
import csv
import io
import fetcher_lib
import logging
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
                last_update_date_str = self.db_client_.ReadLatestDateBySymbol(
                    symbol)
                if last_update_date_str:
                    last_update_date = datetime.datetime.strptime(
                        str(last_update_date_str), '%Y-%m-%d').date()
                else:
                    last_update_date = datetime.date(2000, 1, 1)
                if last_update_date < datetime.date.today():
                    stock_fetcher = fetcher_lib.StockDataFetcher(
                        symbol, last_update_date)
                    time.sleep(0.5)
                    stock_data = stock_fetcher.UpdateStockData()
                    if stock_data is None:
                        continue
                    logging.info('Shard %d updated stock: %s' %
                                 (self.thread_id_, symbol))
                    print('Shard %d updated stock: %s' %
                          (self.thread_id_, symbol))
                    reader = csv.DictReader(io.StringIO(stock_data))
                    for data in reader:
                        if datetime.datetime.strptime(
                                data['Date'], '%Y-%m-%d').date() > last_update_date:
                            self.db_client_.InsertStockData(
                                symbol, data['Date'], open_p=data['Open'], close_p=data['Close'],
                                high_p=data['High'], low_p=data['Low'], volume_p=data['Volume'],
                                open_adj=data['Adj. Open'], close_adj=data['Adj. Close'],
                                high_adj=data['Adj. High'], low_adj=data['Adj. Low'], volume_adj=data['Adj. Volume'],
                                ex_dividend=data['Ex-Dividend'], split_ratio=data['Split Ratio'])


class FetcherManager:
    def __init__(self):
        self.company_list_ = []

    def UpdateCompanyList(self):
        db_client = database_lib.StockzDatabaseClient()
        logging.info('Start update NYSE')
        fetcher_nyse = fetcher_lib.CompanyListFetcher("NYSE")
        company_list = fetcher_nyse.GetList()
        logging.info('NYSE company list:\n' + str(company_list))
        company_list.pop(0)
        for company in company_list:
            db_client.UpdateStockList(
                company[0], [company[1], 0, company[6], company[7]])
        logging.info('Start update NASDAQ')
        fetcher_nasdaq = fetcher_lib.CompanyListFetcher("NASDAQ")
        company_list = fetcher_nasdaq.GetList()
        logging.info('Nasdaq company list:\n' + str(company_list))
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
