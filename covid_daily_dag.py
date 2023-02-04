import os
import requests
import logging
from datetime import datetime
from airflow import DAG
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.hooks.base import BaseHook
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.apache.hive.hooks.hive import HiveCliHook
from airflow.providers.apache.hive.operators.hive import HiveOperator

BASE_ENDPOINT = 'CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/'
HIVE_TABLE = 'COVID_RESULTS'


def download_covid_data(**kwargs):
    conn = BaseHook.get_connection(kwargs['conn_id'])
    url = conn.host + kwargs['endpoint'] + kwargs['exec_date'] + '.csv'
    logging.info(f'Sending get request to COVID19 repository by url: {url}')
    response = requests.get(url)
    if response.status_code == 200:
        save_path = f"/opt/airflow/dags/files/{kwargs['exec_date']}.csv"
        logging.info(f'Successfully get data. Saving to file: {save_path}')
        with open(save_path, 'w') as f:
            f.write(response.text)
    else:
        raise ValueError(f'Unable get data url: {url}')


def check_if_table_exists(**kwargs):
    table = kwargs['table'].lower()
    conn = HiveCliHook(hive_cli_conn_id=kwargs['conn_id'])
    logging.info(f'Checking if hive table {table} exists.')
    query = f"show tables in default like '{table}';"
    logging.info(f'Running query: {query}')
    result = conn.run_cli(hql=query)
    if 'OK' in result:
        if table in result:
            logging.info(f'Table {table} exists. Proceed with adding new partition.')
            return 'load_to_hive'
        else:
            logging.info(f'Table {table} does not exist. Proceed with hive table creation.')
            return 'create_hive_table'
    else:
        raise RuntimeError(f'Hive returned not OK status while running query: {query}')


default_args = {
    'owner': 'student',
    'email': 'student@some-uinversity.edu',
    'email_on_retry': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': 60,  # seconds!
    'depends_on_past': False,
    'start_date': datetime(2022, 12, 1),
    'end_date': datetime(2022, 12, 5)
}

with DAG(
    dag_id='covid_daily_data',
    tags=['practice', 'daily', 'covid19'],
    description="Ежедневная загрузка данных о COVID-19",
    schedule_interval='0 7 * * *',
    max_active_runs=1,
    concurrency=4,
    default_args=default_args,
    user_defined_macros={
        'convert_date': lambda dt: dt.strftime('%m-%d-%Y')
    }
) as main_dag:

    # План ETL
    # 0. Получить дату, за которую нужно забирать данные:
    EXEC_DATE = '{{ convert_date(execution_date) }}'

    # 1. Проверить доступность API и данных
    check_if_data_available = HttpSensor(
        task_id='check_if_data_available',
        http_conn_id='covid_api',
        endpoint=f'{BASE_ENDPOINT}{EXEC_DATE}.csv',
        poke_interval=60,
        timeout=600,
        soft_fail=False,
        mode='reschedule'
    )

    # 2. Загрузить данные (в локальную файловую систему, на сервер аирфлоу)
    download_data = PythonOperator(
        task_id='download_data',
        python_callable=download_covid_data,
        op_kwargs={
            'conn_id': 'covid_api',
            'endpoint': BASE_ENDPOINT,
            'exec_date': EXEC_DATE
        }
    )

    # 3. Переместить эти данные в HDFS. После нужно почистить за собой файл/ы на сервере Airflow
    move_data_to_hdfs = BashOperator(
        task_id='move_data_to_hdfs',
        bash_command="""
            hdfs dfs -mkdir -p /covid_data/csv
            hdfs dfs -copyFromLocal /opt/airflow/dags/files/{exec_date}.csv /covid_data/csv
            rm /opt/airflow/dags/files/{exec_date}.csv
        """.format(exec_date=EXEC_DATE)
    )

    # 4. Обработать данные - Spark Job, который будет агрегировать данные на уровень стран и считать статистики.
    process_data = SparkSubmitOperator(
        task_id='process_data',
        application=os.path.join(main_dag.folder, 'scripts/covid_data_processing.py'),
        conn_id='spark_conn',
        name=f'{main_dag.dag_id}_process_data',
        application_args=[
            '--exec_date', EXEC_DATE
        ]
    )

    # 5. Создать Hive таблицу поверх обработанных данных.
    #    a. Проверить, если ли нужная нам таблица в Hive.
    check_if_hive_table_exists = BranchPythonOperator(
        task_id='check_if_hive_table_exists',
        python_callable=check_if_table_exists,
        op_kwargs={
            'table': HIVE_TABLE,
            'conn_id': 'hive_conn'
        }
    )

    #    b. Если таблица таблица отсутсвует, то нам нужно ее создать.
    create_hive_table = HiveOperator(
        task_id='create_hive_table',
        hive_cli_conn_id='hive_conn',
        hql="""
            CREATE EXTERNAL TABLE IF NOT EXISTS default.{table}(
                country_region STRING,
                total_confirmed INT,
                total_deaths INT,
                fatality_ratio DOUBLE,
                world_case_pct DOUBLE,
                world_death_pct DOUBLE
            )
            PARTITIONED BY (exec_date STRING)
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            STORED AS TEXTFILE
            LOCATION '/covid_data/results';
        """.format(table=HIVE_TABLE)
    )

    #    c. Если таблица есть, то пропустить создание этой таблицы и перейти сразу к обновлению списка партиций.
    # 6. Нужно обновить список партиций в таблице.
    load_to_hive = HiveOperator(
        task_id='load_to_hive',
        hive_cli_conn_id='hive_conn',
        hql=f"MSCK REPAIR TABLE default.{HIVE_TABLE};",
        trigger_rule=TriggerRule.ONE_SUCCESS
    )

    check_if_data_available >> download_data >> move_data_to_hdfs >> process_data >> check_if_hive_table_exists
    check_if_hive_table_exists >> [create_hive_table, load_to_hive]
    create_hive_table >> load_to_hive

