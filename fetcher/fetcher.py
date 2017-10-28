import datetime
import fetcher_manager
import logging
import sys
import time

LOG_DIR = "/tmp/"
UPDATE_SCHEDULE = {1, 3, 5, 18, 20, 22}


def UpdateCompanyListAndStockData():
    logging.info('Update Started')
    fetcher_manager_ = fetcher_manager.FetcherManager()
    logging.info('Update Company List')
    fetcher_manager_.UpdateCompanyList()
    logging.info('Update Stock Data')
    fetcher_manager_.UpdateStockData()
    logging.info('Update completed')


def main():
    if len(sys.argv) == 2 and sys.argv[1] == 'once':
        print('Manually update')
        UpdateCompanyListAndStockData()
    else:
        while True:
            now = datetime.datetime.now()
            if now.hour in UPDATE_SCHEDULE and now.minute <= 1:
                UpdateCompanyListAndStockData()
            time.sleep(40)


if __name__ == '__main__':
    logging.basicConfig(filename=LOG_DIR + 'fetcher.log',
                        format='%(levelname) -10s %(asctime)s %(module)s:%(lineno)s %(funcName)s %(message)s',
                        level=logging.INFO)
    main()
