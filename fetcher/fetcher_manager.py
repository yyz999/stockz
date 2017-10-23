import sys
sys.path.append('../database_api')
import database_lib
import fetcher_lib
import logging


class FetcherManager:
    def __init__(self):
        pass

    def UpdateCompanyList(self):
        db_client = database_lib.StockzDatabaseClient()
        print('Start update NYSE')
        fetcher_nyse = fetcher_lib.CompanyListFetcher("NYSE")
        company_list = fetcher_nyse.GetList()
        logging.info('NYSE company list:\n' + str(company_list))
        company_list.pop(0)
        for company in company_list:
            db_client.UpdateStockList(
                company[0], [company[1], 0, company[6], company[7]])
        print('Start update NASDAQ')
        fetcher_nasdaq = fetcher_lib.CompanyListFetcher("NASDAQ")
        company_list = fetcher_nasdaq.GetList()
        logging.info('Nasdaq company list:\n' + str(company_list))
        company_list.pop(0)
        for company in company_list:
            db_client.UpdateStockList(
                company[0], [company[1], 0, company[6], company[7]])

    def UpdateStockData(self):
        pass
