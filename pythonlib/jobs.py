import logging
import os
import zipfile
from datetime import datetime

import numpy as np
import pandas as pd
from peewee import chunked

from models import StockMaster, StockPrice

import requests

from constants import DATA_DIR
from db import DatabaseSingleton

logger = logging.getLogger('app')


def download_file(url, filepath):
    # Downloads a file from the given URL and saves it to the specified filepath
    try:
        # Define browser-like headers
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Cache-Control': 'max-age=0',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Pragma': 'no-cache',
        }

        response = requests.get(url, headers=headers, stream=True)
        response.raise_for_status()  # Raise an HTTPError for bad responses
        with open(filepath, 'wb') as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)
        logger.info(f"File downloaded successfully: {filepath}")
    except requests.exceptions.RequestException as e:
        logger.error(f"Error downloading file: {e}")
        raise


def chunked_insert_from_dataframe(dataframe, model_class, chunk_size=100):
    data_dict = dataframe.to_dict('records')
    db = DatabaseSingleton.get_db_instance()
    with db.atomic():
        for batch in chunked(data_dict, chunk_size):
            model_class.insert_many(batch).on_conflict_ignore().execute()

        logger.info(f"Inserted {len(data_dict)} records into {model_class.__name__} table.")


def load_stock_master_data():
    # Downloads the stock master data from the NSE website and loads it into the database
    url = "https://archives.nseindia.com/content/equities/EQUITY_L.csv"
    today = datetime.now().strftime("%Y%m%d")
    filepath = os.path.join(DATA_DIR, f"stock_master_data_{today}.csv")
    download_file(url, filepath)

    # Parse CSV and load into the database using pandas and peewee
    # Read the CSV file into a DataFrame
    df = pd.read_csv(filepath)
    logger.info("CSV file read successfully")
    # Clean the DataFrame
    df.columns = df.columns.str.strip()
    df = df.rename(columns={
        'SYMBOL': 'symbol',
        'NAME OF COMPANY': 'name',
        'SERIES': 'series',
        'DATE OF LISTING': 'date_of_listing',
        'PAID UP VALUE': 'paid_up_value',
        'MARKET LOT': 'market_lot',
        'ISIN NUMBER': 'isin_number',
        'FACE VALUE': 'face_value'
    })

    df['date_of_listing'] = pd.to_datetime(df['date_of_listing'], format='%d-%b-%Y', errors='coerce')
    df['paid_up_value'] = pd.to_numeric(df['paid_up_value'], errors='coerce')
    df['market_lot'] = pd.to_numeric(df['market_lot'], errors='coerce')
    df['face_value'] = pd.to_numeric(df['face_value'], errors='coerce')

    chunked_insert_from_dataframe(df, StockMaster, chunk_size=1000)


def load_daily_stock_prices(dated=None):
    # Downloads the daily stock prices from the NSE website and loads it into the database
    today = datetime.now().strftime("%Y%m%d")
    if not dated:
        download_date = today
    else:
        download_date = datetime.strptime(dated, "%Y-%m-%d").strftime("%Y%m%d")

    filename = f"BhavCopy_NSE_CM_0_0_0_{today}_F_0000"
    url = f'https://nsearchives.nseindia.com/content/cm/{filename}.csv.zip'

    filepath = os.path.join(DATA_DIR, f"{filename}.csv.zip")
    download_file(url, filepath)

    # Unzip the downloaded file
    with zipfile.ZipFile(filepath, 'r') as zip_ref:
        unzip_dir = os.path.join(DATA_DIR, filename)
        zip_ref.extractall(unzip_dir)
        logger.info(f"Unzipped file {filename}.csv to {unzip_dir}")

    # Parse CSV and load into the database using pandas and peewee
    # Read the CSV file into a DataFrame
    csv_filename = os.path.join(unzip_dir, f"{filename}.csv")
    df = pd.read_csv(csv_filename)
    logger.info("CSV file read successfully")
    # Clean the DataFrame
    df.columns = df.columns.str.strip()

    # Rename columns to match the database model
    df = df.rename(columns={
        'TradDt': 'trading_date',
        'BizDt': 'biz_date',
        'Sgmt': 'segment',
        'Src': 'source',
        'FinInstrmTp': 'fin_instr_type',
        'FinInstrmId': 'fin_instr_id',
        'ISIN': 'isin_number',
        'TckrSymb': 'symbol',
        'SctySrs': 'instr_series',
        'XpryDt': 'instr_expiry_date',
        'FininstrmActlXpryDt': 'instr_actual_expiry_date',
        'StrkPric': 'instr_strike_price',
        'OptnTp': 'instr_option_type',
        'FinInstrmNm': 'instr_name',
        'OpnPric': 'open_price',
        'HghPric': 'high_price',
        'LwPric': 'low_price',
        'ClsPric': 'close_price',
        'LastPric': 'last_traded_price',
        'PrvsClsgPric': 'prev_close_price',
        'UndrlygPric': 'underlying_asset_price',
        'SttlmPric': 'settlement_price',
        'OpnIntrst': 'open_interest',
        'ChngInOpnIntrst': 'change_in_open_interest',
        'TtlTradgVol': 'total_traded_volume',
        'TtlTrfVal': 'total_traded_value',
        'TtlNbOfTxsExctd': 'total_traded_txn',
        'SsnId': 'session_id',
        'NewBrdLotQty': 'market_lot_size',
        'Rmks': 'remarks'
    })

    df.drop(['Rsvd1', 'Rsvd2', 'Rsvd3', 'Rsvd4'], axis=1, inplace=True)

    df['trading_date'] = pd.to_datetime(df['trading_date'], format='%Y-%m-%d', errors='coerce')
    df['biz_date'] = pd.to_datetime(df['biz_date'], format='%Y-%m-%d', errors='coerce')
    df['fin_instr_id'] = pd.to_numeric(df['fin_instr_id'], errors='coerce')
    df['instr_expiry_date'] = df['instr_expiry_date'].replace({np.nan: None})
    df['instr_actual_expiry_date'] = df['instr_actual_expiry_date'].replace({np.nan: None})

    df['instr_strike_price'] = pd.to_numeric(df['instr_strike_price'], errors='coerce')
    df['instr_strike_price'] = df['instr_strike_price'].replace({np.nan: None})

    df['instr_option_type'] = df['instr_option_type'].replace({np.nan: None})
    df['open_price'] = pd.to_numeric(df['open_price'], errors='coerce')
    df['high_price'] = pd.to_numeric(df['high_price'], errors='coerce')
    df['low_price'] = pd.to_numeric(df['low_price'], errors='coerce')
    df['close_price'] = pd.to_numeric(df['close_price'], errors='coerce')
    df['last_traded_price'] = pd.to_numeric(df['last_traded_price'], errors='coerce')
    df['prev_close_price'] = pd.to_numeric(df['prev_close_price'], errors='coerce')

    df['underlying_asset_price'] = pd.to_numeric(df['underlying_asset_price'], errors='coerce')
    df['underlying_asset_price'] = df['underlying_asset_price'].replace({np.nan: None})

    df['settlement_price'] = pd.to_numeric(df['settlement_price'], errors='coerce')

    df['open_interest'] = pd.to_numeric(df['open_interest'], errors='coerce')
    df['open_interest'] = df['open_interest'].replace({np.nan: None})

    df['change_in_open_interest'] = pd.to_numeric(df['change_in_open_interest'], errors='coerce')
    df['change_in_open_interest'] = df['change_in_open_interest'].replace({np.nan: None})

    df['total_traded_volume'] = pd.to_numeric(df['total_traded_volume'], errors='coerce')
    df['total_traded_value'] = pd.to_numeric(df['total_traded_value'], errors='coerce')
    df['total_traded_txn'] = pd.to_numeric(df['total_traded_txn'], errors='coerce')
    df['market_lot_size'] = pd.to_numeric(df['market_lot_size'], errors='coerce')

    df['remarks'] = df['remarks'].replace({np.nan: None})

    chunked_insert_from_dataframe(df, StockPrice, chunk_size=1000)
