# lib for fetcher
import logging
import urllib.request
from socket import timeout
import quandl
import datetime


class CompanyListFetcher:
    def __init__(self, market_name):
        self._market_name_ = market_name

    def GetList(self):
        NASDAQ_URL = "http://www.nasdaq.com/screening/companies-by-industry.aspx?exchange=NASDAQ&render=download"
        NYSE_URL = "http://www.nasdaq.com/screening/companies-by-industry.aspx?exchange=NYSE&render=download"
        company_table = list()
        market_url = None
        if self._market_name_ == "NASDAQ":
            market_url = NASDAQ_URL
        elif self._market_name_ == "NYSE":
            market_url = NYSE_URL
        else:
            logging.warning("Unknown market name " + self._market_name_)
            return None
        try:
            byte_stream = urllib.request.urlopen(market_url).read()
        except (HTTPError, URLError) as error:
            logging.error(
                'Fail to retrieve list form %s because %s\n', self._market_name_, error)
        except timeout:
            logging.error(
                'Fail to retrieve list form %s because of timed out.', self._market_name_)
        else:
            logging.info('Retrieving company list from %s succeed.',
                         self._market_name_)
        content = byte_stream.decode("utf8")
        content = content.split("\r\n")
        for company in content:
            if company == '':
                continue
            tmp = company[1:-2].split("\",\"")
            if len(tmp) != 9:
                logging.error("Fail to parse company list: %s", company)
            else:
                company_table.append(tmp)
        return company_table


class StockDataFetcher:
    def __init__(self, symbol, last_update_date):
        self.symbol_ = symbol.upper()
        self.start_date_ = last_update_date + datetime.timedelta(days=1)
        quandl.ApiConfig.api_key = "yCpLwy5AWwP_LpMjs4U8"

    def UpdateStockData(self):
        try:
            response_data = quandl.get_table(
                "WIKI/PRICES", ticker=self.symbol_,
                date={'gte': str(self.start_date_), 'lte': str(datetime.date.today())})
            if len(response_data.to_dict()['date']) == 0:
                return None
        except:
            logging.info('StockDataFetcher failed to update: %s' %
                         (self.symbol))
            return None
        return response_data.to_csv()
