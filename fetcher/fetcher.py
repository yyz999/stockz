import fetcher_manager
import logging
import time
# main
LOG_DIR = "/tmp/"


def main():
    logging.basicConfig(filename=LOG_DIR + 'fetcher.log',
                        format='%(levelname) -10s %(asctime)s %(module)s:%(lineno)s %(funcName)s %(message)s',
                        level=logging.INFO)
    while True:
        logging.info('Update Started')
        fetcher_manager_ = fetcher_manager.FetcherManager()
        logging.info('Update Company List')
        fetcher_manager_.UpdateCompanyList()
        logging.info('Update Stock Data')
        fetcher_manager_.UpdateStockData()
        logging.info('Finished')
        time.sleep(60 * 60 * 4)


if __name__ == '__main__':
    main()
