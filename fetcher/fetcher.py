import fetcher_lib
import logging

# main
LOG_DIR = ""


def main():
    logging.basicConfig(filename=LOG_DIR + 'fetcher.log', level=logging.INFO)
    logging.info('Started')
    # Do something
    logging.info('Finished')


if __name__ == '__main__':
    main()
