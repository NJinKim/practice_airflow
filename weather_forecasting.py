from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook

from datetime import datetime
from datetime import timedelta
# from plugins import slack

import requests
import logging
import psycopg2

def get_Redshift_connection(autocommit=False):
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()

def extract(**context):
    lat = 37.3387
    lon = 121.8853
    key = context["params"]["key"]
    link = f"https://api.openweathermap.org/data/2.5/onecall?lat={lat}&lon={lon}&appid={key}&units=metric"

    task_instance = context['task_instance']
    execution_date = context['execution_date']

    logging.info(execution_date)
    f = requests.get(link)

    return (f.json()['daily'])

def transform(**context):
    text = context["task_instance"].xcom_pull(key="return_value", task_ids="extract")
    lines = []
    for t in text:
        date, temp, min_temp, max_temp = datetime.fromtimestamp(t["dt"]).strftime('%Y-%m-%d'),t["temp"]["day"], t["temp"]["max"], t["temp"]["min"]
        values = (date, temp, min_temp, max_temp)
        lines.append(values)
    return lines

def load(**context):
    schema = context["params"]["schema"]
    table = context["params"]["table"]
    
    cur = get_Redshift_connection()
    lines = context["task_instance"].xcom_pull(key="return_value", task_ids="transform")
    sql = """BEGIN; DROP TABLE IF EXISTS {schema}.{table};
            CREATE TABLE {schema}.{table}(
              date date primary key,
              temp float,
              min_temp float,
              max_temp float,
              created_date timestamp default GETDATE()
            );
          """.format(schema=schema, table=table)
    for line in lines:
        if line != "":
            date, temp, min_temp, max_temp = line[0], line[1], line[2], line[3]
            sql += f"""INSERT INTO {schema}.{table} VALUES ('{date}', '{temp}', '{min_temp}', '{max_temp}'');"""
    sql += "END;"
    logging.info(sql)
    cur.execute(sql)
    
    
dag_third_assignment = DAG(
    dag_id = 'open_weather_dag',
    start_date = datetime(2023,4,6), # 날짜가 미래인 경우 실행이 안됨
    schedule = '0 2 * * *',  # 적당히 조절
    max_active_runs = 1,
    catchup = False,
    default_args = {
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
        # 'on_failure_callback': slack.on_failure_callback,
    }
)

extract = PythonOperator(
    task_id = 'extract',
    python_callable = extract,
    params = {
        "key": Variable.get("open_weather_api_key")
    },
    dag = dag_third_assignment)

transform = PythonOperator(
    task_id = 'transform',
    python_callable = transform,
    params = { 
    },  
    dag = dag_third_assignment)

load = PythonOperator(
    task_id = 'load',
    python_callable = load,
    params = {
        'schema': 'ektlsns',   
        'table': 'weather_forecast'
    },
    dag = dag_third_assignment)

extract >> transform >> load
