from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta

import yfinance as yf
import pandas as pd
import logging

# DB connections
def get_Redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()

# Table Drop / Create
def create_table(cur, schema, table, drop_first):
    if drop_first:
        cur.execute(f"DROP TABLE IF EXISTS {schema}.{table};")
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {schema}.{table} (
            date date,
            name varchar(100),
            open_value float,
            close_value float,
            volume bigint
        );
        """
    )

# Get symbol data 
@task
def get_historical_prices(symbols):
    records = []
    for name, symbol in symbols.items():
        ticket = yf.Ticker(symbol)
        data = ticket.history(period="1d") # 1d 기준

        for index, row in data.iterrows():
            records.append([
                    index.strftime("%Y-%m-%d"),
                    name,
                    row["Open"],
                    row["Close"],
                    row["Volume"]
                ])
    logging.info("Get symbol data")
    return records

# load data
@task
def load(schema, table, records):
    logging.info("Start load")
    cur = get_Redshift_connection() # DB
    try:
        cur.execute("BEGIN;")
        # Create table 
        create_table(cur, schema, table, False)

        # Insert 일자별 데이터
        for r in records:
            # 데이터를 저장하기 전에 NaN 체크
            open_value = f"NULL" if pd.isna(r[2]) else f"ROUND({r[2]}, 2)"
            close_value = f"NULL" if pd.isna(r[3]) else f"ROUND({r[3]}, 2)"
            volume = f"NULL" if pd.isna(r[4]) else f"{r[4]}"
            
            sql = f"""
                INSERT INTO {schema}.{table} (name, date, open_value, close_value, volume)
                SELECT '{r[1]}', '{r[0]}', ROUND({r[2]}, 2), ROUND({r[3]}, 2), {r[4]}
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM {schema}.{table}
                    WHERE name = '{r[1]}' AND date = '{r[0]}'
                );
            """
            cur.execute(sql)

        cur.execute("COMMIT;")

    except Exception as error:
        print(error)
        cur.execute("ROLLBACK;")
        raise

    logging.info("End load")


with DAG(
    dag_id = 'rawmaterials_dag',
    start_date = datetime(2023,1,1),
    catchup=True,
    max_active_runs=1,
    tags=['API'],
    schedule = '0 15 * * 0'
) as dag:
    # 자산 심볼 정의
    symbols = {
        "원유": "CL=F",
        "천연가스": "NG=F",
        "금": "GC=F",
        "은": "SI=F",
        "구리": "HG=F",  
        "휘발유": "RB=F",   
        "경유": "HO=F",
        "옥수수": "ZC=F",
        "커피": "KC=F",
        "설탕": "SB=F"
}
    results = get_historical_prices(symbols)
    load("shyun0830", "rawmaterials", results)