import fetcher_manager
import logging

# main
LOG_DIR = "/tmp/"


def main():
    logging.basicConfig(filename=LOG_DIR + 'fetcher.log', level=logging.INFO)
    logging.info('Started')
    fetcher_manager_ = fetcher_manager.FetcherManager()
    fetcher_manager_.UpdateCompanyList()
    logging.info('Finished')


if __name__ == '__main__':
    main()
