from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from airflow.decorators import task
from airflow.operators.python import get_current_context

from datetime import datetime
from datetime import timedelta

import pandas as pd
import requests
import logging
import json
import pytz
import time

# KST 타임존 설정 (Asia/Seoul)
kst = pytz.timezone('Asia/Seoul')

def get_Redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()

def get_last_monday():
    # DAG 돌리는 시점의 date = Sunday
    context = get_current_context()
    execution_date = context['data_interval_start'] 

    # Calculate days since last Monday (Monday = 0, Sunday = 6)
    days_since_monday = execution_date.weekday()
    last_monday = execution_date - timedelta(days=days_since_monday)

    return last_monday

def get_next_monday():  # renamed function
    # DAG 돌리는 시점의 date = Sunday
    context = get_current_context()
    execution_date = context['data_interval_start']

    # Calculate days until next Monday (Monday = 0, Sunday = 6)
    days_until_monday = (7 - execution_date.weekday()) % 7
    next_monday = execution_date + timedelta(days=days_until_monday)

    return next_monday

def get_date_range():
    #last_monday = get_last_monday()
    next_monday = get_next_monday()
    dates = []
    
    # Generate dates from Monday to Friday (5 days)
    for i in range(7):  # 0 to 6, representing Monday to Sunday
        #current_date = last_monday + timedelta(days=i)
        current_date = next_monday + timedelta(days=i)
        dates.append(current_date.strftime("%Y-%m-%d"))
    
    return dates

def create_empty_data(template, date):
    """
    Create empty exchange rate data using the template structure
    Args:
        template: List of currency dictionaries to use as template
        date: String date in format 'YYYY-MM-DD'
    Returns:
        List of dictionaries with empty values except for currency info and date
    """
    empty_data = []
    for currency in template:
        empty_row = {
            'result': None,
            'cur_unit': currency['cur_unit'],
            'ttb': None,
            'tts': None,
            'deal_bas_r': None,
            'bkpr': None,
            'yy_efee_r': None,
            'ten_dd_efee_r': None,
            'kftc_bkpr': None,
            'kftc_deal_bas_r': None,
            'cur_nm': currency['cur_nm'],
            'date': date
        }
        empty_data.append(empty_row)

    return empty_data

@task
def extract_koreaexim_currency(api_key):
    logging.info("Extract started")

    exchange_rates = []
    dates = get_date_range()
    template = None

    for search_date in dates:
        try:
            url = f"https://www.koreaexim.go.kr/site/program/financial/exchangeJSON?authkey={api_key}&searchdate={search_date}&data=AP01"
            response = requests.get(url)

            response.raise_for_status()  # Raises an HTTPError for bad responses

            data = json.loads(response.text)

            # Convert search_date to datetime for weekday check
            current_date = datetime.strptime(search_date, "%Y-%m-%d")
            is_weekend = current_date.weekday() >= 5  # 5 is Saturday, 6 is Sunday
            # If data is empty and we have a template
            if not data and template:
                data = create_empty_data(template, search_date)
            elif data:  # If we get valid data, update template
                template = data
                if is_weekend:  # If it's weekend, keep Friday's template
                    template = exchange_rates[-2]['data'] if len(exchange_rates) >= 2 else data

            exchange_rates.append({
                'date': search_date,
                'data': data
            })

            time.sleep(2)  # Add a 2-second delay between requests

        except Exception as e:
            # TODO: need to add error handling for airflow
            print(f"Error fetching data for {search_date}: {str(e)}")
            raise

    logging.info("Extract done")

    return exchange_rates

@task
def transform_koreaexim_currency(data):
    logging.info("Transform started")
    # Flatten the nested data
    flattened_data = []
    for daily_rate in data:
        date = daily_rate['date']
        for currency in daily_rate['data']:
            # 한국 통화는 저장 pass
            if currency['cur_unit'] == 'KRW':
                continue
            # Split cur_nm into Korean name and country
            elif currency['cur_unit'] == 'CNH':
                country_kr = '중국'
                name_kr = '위안'
            elif currency['cur_unit'] == 'EUR':
                country_kr = '유럽'
                name_kr = '유로'
            elif currency['cur_nm']:
                name_parts = currency['cur_nm'].split(' ')
                country_kr = name_parts[0]
                name_kr = ' '.join(name_parts[1:]) if len(name_parts) > 1 else ''
            else:
                country_kr = None
                name_kr = None
            
            # Convert string values to float after removing commas
            ttb = float(currency['ttb'].replace(',', '')) if currency['ttb'] else None
            tts = float(currency['tts'].replace(',', '')) if currency['tts'] else None
            kftc_deal_base_r = float(currency['kftc_deal_bas_r'].replace(',', '')) if currency['kftc_deal_bas_r'] else None
            kftc_bkpr = float(currency['kftc_bkpr'].replace(',', '')) if currency['kftc_bkpr'] else None
        
            # Create new row with only desired columns
            row = {
                'date': date,
                'name': currency['cur_unit'],
                'name_kr': name_kr,
                'country_kr': country_kr,
                'ttb': ttb,
                'tts': tts,
                'kftc_deal_base_r': kftc_deal_base_r,
                'kftc_bkpr': kftc_bkpr
            }

            flattened_data.append(row)

    # Convert to DataFrame
    transformed_exchange_rates = pd.DataFrame(flattened_data)

    logging.info("Transform ended")
    return transformed_exchange_rates

def _create_table(cur, schema, table, drop_first):
    try:
        if drop_first:
            cur.execute(f"DROP TABLE IF EXISTS {schema}.{table};")
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {schema}.{table} (
                date DATE,
                name VARCHAR(100) NOT NULL,
                name_kr VARCHAR(100),
                country_kr VARCHAR(100),
                ttb FLOAT,
                tts FLOAT,
                kftc_deal_base_r FLOAT,
                kftc_bkpr FLOAT
            );""")
        print(f"Table {schema}.{table} created successfully.")
    except Exception as e:
        print(f"Error creating table {schema}.{table}: {e}")
        raise

@task
def load_koreaexim_currency(schema, table, records):
    logging.info("Load started")    
    # connect to db
    cur = get_Redshift_connection()

    try:
        cur.execute("BEGIN;")
        _create_table(cur, schema, table, False)

        # Use parameterized query
        insert_sql = f"""
            INSERT INTO {schema}.{table} 
            (date, name, name_kr, country_kr, ttb, tts, kftc_deal_base_r, kftc_bkpr)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
        """

        for _, row in records.iterrows():
            values = (
                pd.to_datetime(row['date']).date(),
                row['name'],
                row['name_kr'],
                row['country_kr'],
                row['ttb'],
                row['tts'],
                row['kftc_deal_base_r'],
                row['kftc_bkpr']
            )
            cur.execute(insert_sql, values)

        cur.execute("COMMIT;")

    except Exception as error:
        print(error)
        cur.execute("ROLLBACK;")
        raise
    
    logging.info("Load ended")

with DAG(
    dag_id='krwcurrency_dag',
    start_date=datetime(2023, 1, 1, tzinfo=kst),
    catchup=True,
    schedule='0 15 * * 0',
    max_active_runs=1,
) as dag:

    # 데이터를 가져오는 task
    api_key = Variable.get("koreaexim_api_key")
    results = extract_koreaexim_currency(api_key)

    # 데이터를 sql로 적재 편하도록 변환하는 task
    records = transform_koreaexim_currency(results)

    # Redshift에 데이터 로드하는 task
    load_koreaexim_currency(Variable.get("redshift_schema_name"), "krwcurrency", records)