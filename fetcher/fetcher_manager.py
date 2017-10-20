import sys
sys.path.append('../database_api')
import database_lib
import fetcher_lib
import logging


class FetcherManager:
    def __init__(self):
        pass

    def UpdateCompanyList(self):
        fetcher_nasdaq = fetcher_lib.CompanyListFetcher("NASDAQ")
        fetcher_nyse = fetcher_lib.CompanyListFetcher("NYSE")
        db_client = database_lib.StockzDatabaseClient()
        company_list = fetcher_nasdaq.GetList()
        company_list.pop(0)
        for company in company_list:
            db_client.UpdateStockList(
                company[0], [company[1], 0, company[6], company[7]])
        company_list = fetcher_nyse.GetList()
        company_list.pop(0)
        for company in company_list:
            db_client.UpdateStockList(
                company[0], [company[1], 0, company[6], company[7]])
