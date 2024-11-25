from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook

from datetime import datetime
import os

# !!WIP!!
# 레딧 잡을 실행하는 대그 체인중 첫 대그입니다. 앞선 대그들이 모두 실행 완료되면 작동합니다.
# 레딧 잡 실행을 위해서 필요한 패키지 등을 세팅해주는 역할을 합니다.
# TODO: YARS 프록시 요청 테스트, YARS 파라미터 테스트

def setup_dbt_profiles():
    """
    DBT가 사용할 컨피그를 다이나믹하게 생성해주는 메서드입니다.
    """
    # 유효한 정보 확인 후 입력
    conn = BaseHook.get_connection("redshift_connection")
    profiles_content = f"""
    default:
        outputs:
            dev:
                type: redshift
                host: {conn.host}
                port: {conn.port}
                user: {conn.login}
                password: {conn.password}
                dbname: {conn.schema}
                schema: {conn.login}
                threads: 1
        target: dev
    """
    os.makedirs("~/.dbt", exist_ok=True)
    with open("~/.dbt/profiles.yml", "w") as f:
        f.write(profiles_content)

default_args = {
    'start_date': datetime(2024,11, 1),
    'retries': 1
}

with DAG(
    'reddit_dag_init',
    default_args=default_args,
    schedule_interval=None, # 다른 대그가 트리거 함
    catchup=False
) as dag:
    
    # task1: 전 단계 대그 완료 대기(원자재, 주식, 환율 등 관련 대그 모두 완료 후)
    wait_for_prerequisite_dag = ExternalTaskSensor(
        task_id='wait_for_prerequisite_dag',
        external_dag_id='prerequisite_dag', # prone to change
        external_task_id=None,
        mode='poke',
        timeout=600,
        poke_interval=30
    )

    # task2: 패키지 설치
    # WIP
    # TODO: 패커맨드 테스트중. yars 기능 테스트중
    install_dependencies = BashOperator(
        task_id='install_dependencies',
        bash_command=(
            "git clone https://github.com/datavorous/YARS.git", # YARS 설치
            "pip install -y dbt-redshift uv requests Pygments" # dbt 및 yars 디펜던시
        )
    ) 
