import argparse
import logging
import os.path

from jobs import load_stock_master_data, load_daily_stock_prices
from db import DatabaseSingleton
from models import StockMaster, StockPrice

logger = logging.getLogger('app')


def setup_logging():
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        '[%(asctime)s] %(funcName)s:%(filename)s:%(lineno)d - %(levelname)s >>> %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.debug('Logging is set up')


def start_run():
    parser = argparse.ArgumentParser(
        description='This will run the various jobs to load daily stock data'
    )
    parser.add_argument(
        '-m', '--mode', type=str, required=True, help='The type of load to perform',
        choices=['stockMaster','stockPrice'],
    )
    parser.add_argument(
        '-f', '--filepath', type=str, required=False, help='The file to load',
    )
    args = parser.parse_args()

    # Switch on the mode
    if args.mode == 'stockMaster':
        logger.info('Running stockMaster load')
        if args.filepath:
            logger.info(f'File path provided: {args.filepath}')
            if not os.path.isfile(args.filepath):
                logger.error(f'File not found: {args.filepath}')
                exit(1)

        load_stock_master_data()
    elif args.mode == 'stockPrice':
        logger.info('Running stockPrice load')
        if args.filepath:
            logger.info(f'File path provided: {args.filepath}')
            if not os.path.isfile(args.filepath):
                logger.error(f'File not found: {args.filepath}')
                exit(1)

        load_daily_stock_prices()
    else:
        logger.error(f'Unknown mode: {args.mode}')
        exit(1)


def create_tables():
    db = DatabaseSingleton.get_db_instance()
    db.create_tables([
        StockMaster, StockPrice
    ], safe=True)


if __name__ == "__main__":
    setup_logging()
    create_tables()
    start_run()
