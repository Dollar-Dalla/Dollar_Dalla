from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from plugins.getSymbols import fetch_symbols_from_yahoo

from datetime import datetime, timedelta
import yfinance as yf
import logging, ast

# Constants
CATCHUP = True
ARGS = {
    'owner': 'airflow',
    'start_date': datetime(2024, 11, 10),
    'description': 'An ETL DAG for yfinance world indices data.',
    'trigger_rule': 'all_success',
}

# Helper functions
## DB 커넥션
def get_rs_conn(autocommit=True):
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()

## 테이블 init
def init_table(cur, schema, table, catchup):
    if not catchup:
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


# Task methods
## T1: yfinance와 크롤링한 심볼-이름 데이터를 extract, transform하는 메소드
def fetch_market_data(start, end):
    """
    Parameter:
    symbol dict { symbol: symbol_name }
    """
    # 원래 별개 태스크였는데 xcom 파싱문제떄문에 가져옴
    symbols = fetch_symbols_from_yahoo()
    extracted = []
    for e in symbols:
        ticket = yf.Ticker(e[0])
        data = ticket.history(start=start, end=end)
        for index, row in data.iterrows():
            extracted.append([
                index.strftime('%Y-%m-%d'),
                e[1],
                row['Open'],
                row['Close'],
                row['Volume']
            ])
    logging.info("T1: Extranction-transformation done.")
    return extracted

## T2: DB에 데이터를 로드 해주는 메서드입니다.
def load(schema, table, extracted, catchup):
    cur = get_rs_conn()
    try:
        # 테이블 확인 후 시작
        cur.execute("BEGIN;")
        init_table(cur, schema, table, catchup)
        extracted = ast.literal_eval(extracted)
        for e in extracted: # 날짜, 이름, 심볼, 오픈, 클로즈, 볼륨
            query = f"""
                INSERT INTO {schema}.{table} 
                SELECT '{e[0]}', '{e[1]}', ROUND({e[2]}, 2), ROUND({e[3]}, 2), {e[4]}
                WHERE NOT EXISTS (
                    SELECT 1 FROM {schema}.{table}
                    WHERE name='{e[1]}' AND date = '{e[0]}'
                );
            """
            cur.execute(query)
            # e[0] 이 금욜이면 -> 토일값 + 해서 넣기
            query = ""
            d = datetime.strptime(e[0], "%Y-%m-%d")
            if d.weekday() == 4:
                query = f"""
                    INSERT INTO {schema}.{table} 
                    VALUES ('{d + timedelta(days=1)}', '{e[1]}', null, null, null)
                    , ('{d + timedelta(days=2)}', '{e[1]}', null, null, null)
                """
                cur.execute(query)

            # 공휴일
        cur.execute("COMMIT;")

    except Exception as e:
        logging.error(e)
        cur.execute("ROLLBACK;")
        raise
    
    logging.info("T2: Loading done.")

with DAG(
    dag_id='stockmarket_dag',
    catchup=CATCHUP,
    schedule_interval='0 15 * * 0',
    default_args=ARGS,
    max_active_runs=1
) as dag:

    t1 = PythonOperator(
        task_id='process_yfinance_data',
        python_callable=fetch_market_data,
        provide_context=True,
        op_args=[
            "{{ ds }}",
            "{{ (execution_date + macros.timedelta(days=7)).strftime('%Y-%m-%d') }}"
        ],
    )

    t2 = PythonOperator(
        task_id='push_to_redshift',
        python_callable=load,
        op_args=[
            'tunacome',
            'test_stockmarket',
            '{{ task_instance.xcom_pull(task_ids="process_yfinance_data") }}',
            CATCHUP
        ]
    )

    t1 >> t2