from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

import pandas as pd
import yfinance as yf
import logging


def get_Redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()


@task
def get_historical_prices(symbols, start_date, end_date):
    records = []
    all_dates = pd.date_range(start=start_date, end=end_date, freq='D').strftime("%Y-%m-%d").tolist()

    for name, symbol in symbols.items():
        ticket = yf.Ticker(symbol)
        data = ticket.history(start=start_date, end=end_date)
        
        fetched_dates = set()
        for index, row in data.iterrows():
            date_str = index.strftime("%Y-%m-%d")
            fetched_dates.add(date_str)
            records.append([
                date_str,
                name, 
                row["Open"], 
                row["Close"],
                row["Volume"]
            ])

        missing_dates = set(all_dates) - fetched_dates
        for missing_date in missing_dates:
            records.append([missing_date, name, None, None, None])

    return records


@task
def load(schema, table, records):
    logging.info("load started")
    cur = get_Redshift_connection()
    try:
        # full refresh
        cur.execute("BEGIN;")
        cur.execute(f"DROP TABLE IF EXISTS {schema}.{table};")
        cur.execute(f"""
        CREATE TABLE {schema}.{table} (
            date date,
            name varchar(20),
            open_value float,
            close_value float,
            volume bigint
        );""")
        
        # 
        for r in records:
            sql = f"INSERT INTO {schema}.{table} VALUES (%s, %s, %s, %s, %s);"
            print(sql)
            cur.execute(sql, r)

        cur.execute("COMMIT;")

    except Exception as error:
        print(error)
        cur.execute("ROLLBACK;")
        raise

    logging.info("load done")


with DAG(
    dag_id = 'forex_rate',
    start_date = datetime(2023,1,8),
    catchup=False,
    tags=['API'],
    schedule = '0 15 * * 0'
) as dag:
    # 자산 심볼 정의
    symbols = {
    "EUR/USD": "EURUSD=X",       
    "USD/JPY": "JPY=X", 
    "GBP/USD": "GBPUSD=X",     
    "AUD/USD": "AUDUSD=X",   
    "NZD/USD": "NZDUSD=X",     
    "EUR/JPY": "EURJPY=X",  
    "GBP/JPY": "GBPJPY=X",  
    "EUR/GBP": "EURGBP=X", 
    "EUR/CAD": "EURCAD=X",
    "EUR/SEK": "EURSEK=X",
    "EUR/CHF": "EURCHF=X",
    "EUR/HUF": "EURHUF=X",
    "USD/CNY": "CNY=X",
    "USD/HKD": "HKD=X",
    "USD/SGD": "SGD=X",
    "USD/INR": "INR=X",
    "USD/MXN": "MXN=X", 
    "USD/PHP": "PHP=X", 
    "USD/IDR": "IDR=X", 
    "USD/THB": "THB=X", 
    "USD/MYR": "MYR=X", 
    "USD/ZAR": "ZAR=X", 
    "USD/RUB": "RUB=X", 
    }

    start_date = "2023-01-01"
    end_date = "{{ ds }}"

    results = get_historical_prices(symbols, start_date, end_date)
    load("eumjungmin1", "forex_rate", results)
    