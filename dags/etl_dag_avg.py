from sqlalchemy import create_engine, text
import pandas as pd
import os
from urllib.parse import quote_plus
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

user='2024_mariano_gomez'
password = quote_plus(os.environ.get("REDSHIFT_PASSWORD"))
host='redshift-pda-cluster.cnuimntownzt.us-east-2.redshift.amazonaws.com'
port='5439'
dbname='pda'

REDSHIFT_CONN_STRING = f'redshift+psycopg2://{user}:{password}@{host}:{port}/{dbname}'
DATA_PATH = os.path.dirname(os.path.realpath(__file__))
REDSHIFT_TABLE = 'aqi_metrica_historica'
SCHEMA = "2024_mariano_gomez_schema"

def e_avg(**kwargs):
    output_parquet = kwargs['output_parquet']
    redshift_conn_string = kwargs['redshift_conn_string']
    redshift_historico = kwargs['redshift_historico']
    try:
        engine = create_engine(redshift_conn_string)
        df = pd.read_sql(f'SELECT city,AVG(max_aqius)as "avg_3_aqius", AVG(max_aqicn) as "avg_3_aqicn", MAX(fecha) as "fecha_avg" FROM "2024_mariano_gomez_schema".{redshift_historico} where fecha > (CURRENT_DATE)-4 group by city', engine)
    except Exception as e:
        print(f"Error: {e}")
    
    path = os.path.join(output_parquet, 'avg_3.parquet')
    df.to_parquet(path, index=False)
    
    return path

# Define DAG
with DAG(
    'e_avg',
    default_args={
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
    },
    description='Extract AVG form aqi_metrica_historica ',
    schedule_interval='1 0 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    # Task 1: Extract data from aqi_metrica_historica
    extract_task = PythonOperator(
        task_id='e_avg',
        python_callable=e_avg,
        op_kwargs={'output_parquet': DATA_PATH,
                   'redshift_conn_string': REDSHIFT_CONN_STRING,
                   'redshift_historico': REDSHIFT_TABLE},
    )