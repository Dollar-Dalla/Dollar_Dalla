from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import yfinance as yf
import pandas as pd
import logging


# DB 연결 함수
def get_Redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()


# 테이블 생성 함수
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
        """)

# 날짜 범위
@task
def get_date_range(execution_date_str):
    # execution_date 기준으로 지난 일주일 범위 계산
    execution_date = datetime.strptime(execution_date_str, "%Y-%m-%d")
    start_date = (execution_date - timedelta(days=6)).strftime("%Y-%m-%d")
    end_date = execution_date.strftime("%Y-%m-%d")
    return {"start_date": start_date, "end_date": end_date}


# 데이터 추출 태스크
@task
def get_historical_prices(symbols, date_range):
    start_date = date_range["start_date"]
    end_date = date_range["end_date"]
    records = []
    for name, symbol in symbols.items():
        ticket = yf.Ticker(symbol)
        data = ticket.history(start=start_date, end=end_date)

        for index, row in data.iterrows():
            records.append([
                index.strftime("%Y-%m-%d"),  # Date
                name,  # Asset name
                row["Open"],
                row["Close"],
                row["Volume"]
            ])
    logging.info(f"Retrieved {len(records)} records for {start_date} to {end_date}")
    return records


# 데이터 적재 태스크
@task
def load(schema, table, records):
    logging.info("Start load")
    cur = get_Redshift_connection()  # Redshift DB 연결
    try:
        cur.execute("BEGIN;")
        # 테이블 생성
        create_table(cur, schema, table, False)

        # 레코드 삽입
        for r in records:
            # NaN 값 확인 및 NULL 처리
            open_value = "NULL" if pd.isna(r[2]) else f"ROUND({r[2]}, 2)"
            close_value = "NULL" if pd.isna(r[3]) else f"ROUND({r[3]}, 2)"
            volume = "NULL" if pd.isna(r[4]) else f"{r[4]}"
            
            sql = f"""
                INSERT INTO {schema}.{table} (name, date, open_value, close_value, volume)
                SELECT '{r[1]}', '{r[0]}', {open_value}, {close_value}, {volume}
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM {schema}.{table}
                    WHERE name = '{r[1]}' AND date = '{r[0]}'
                );
            """
            cur.execute(sql)

        cur.execute("COMMIT;")
    except Exception as error:
        logging.error(error)
        cur.execute("ROLLBACK;")
        raise
    logging.info("End load")


with DAG(
    dag_id='rawmaterials_dag',
    start_date=datetime(2023, 1, 1),
    catchup=True,
    max_active_runs=1,
    tags=['API'],
    schedule='0 15 * * 0'  # 매주 일요일 15:00 실행
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

    # 태스크 정의
    date_range = get_date_range('{{ ds }}')  # Use `{{ ds }}` for execution_date in string format
    results = get_historical_prices(symbols, date_range)
    load("shyun0830", "rawmaterials", results)
