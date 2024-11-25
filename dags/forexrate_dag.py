from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta

import pandas as pd
import yfinance as yf
import logging


def get_Redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()


def get_date_range(start_date, end_date):
    # start_date와 end_date는 문자열이므로 datetime으로 변환
    start_date_obj = datetime.strptime(start_date, "%Y-%m-%d")
    end_date_obj = datetime.strptime(end_date, "%Y-%m-%d")
    
    # 날짜 범위 생성 (end_date는 포함되지 않도록 설정)
    date_list = pd.date_range(start=start_date_obj, end=end_date_obj - timedelta(days=1)).strftime("%Y-%m-%d").tolist()
    
    return date_list


@task
def get_historical_prices(symbols, **context):
    start_date = context['ds']
    end_date = (datetime.strptime(start_date, "%Y-%m-%d") + timedelta(days=7)).strftime("%Y-%m-%d")
    all_dates = get_date_range(start_date, end_date)
    print("======================================")
    print("start_date: ", start_date)
    print("end_date = execution_date = ds: ", end_date)
    print("all_dates: ", all_dates)
    print("======================================")

    records = []
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


def _create_table(cur, schema, table, drop_first):
    if drop_first:
        cur.execute(f"DROP TABLE IF EXISTS {schema}.{table};")
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {schema}.{table} (
            date date NOT NULL,
            name varchar(20) NOT NULL,
            open_value float,
            close_value float,
            volume bigint
        );""")


@task
def load(schema, table, records):
    logging.info("load started")
    cur = get_Redshift_connection()
    try:
        cur.execute("BEGIN;")
        _create_table(cur, schema, table, False)

        # 
        for r in records:
            sql = f"""
                INSERT INTO {schema}.{table} (date, name, open_value, close_value, volume)
                SELECT %s, %s, %s, %s, %s
                WHERE NOT EXISTS (
                    SELECT 1 FROM {schema}.{table} WHERE date = %s AND name = %s 
                );
            """
            cur.execute(sql, (*r, r[0], r[1]))

        cur.execute("COMMIT;")

    except Exception as error:
        print(error)
        cur.execute("ROLLBACK;")
        raise

    logging.info("load done")


with DAG(
    dag_id='forex_rate',
    start_date=datetime(2023, 1, 1),
    catchup=True,
    tags=['API'],
    schedule='0 15 * * 0',
    max_active_runs=1, 
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

    results = get_historical_prices(symbols)
    load("eumjungmin1", "forex_rate", results)
