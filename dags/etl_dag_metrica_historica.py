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
REDSHIFT_METRICA = 'aqi_metrica_diaria'
REDSHIFT_HISTORICA = 'aqi_metrica_historica'
SCHEMA = "2024_mariano_gomez_schema"

def e_metrica(**kwargs):
    redshift_conn_string = kwargs['redshift_conn_string']
    redshift_metrica = kwargs['redshift_metrica']
    output_parquet = kwargs['output_parquet']
    try:
        engine = create_engine(redshift_conn_string)
        df = pd.read_sql(f'SELECT * FROM "2024_mariano_gomez_schema".{redshift_metrica}', engine)
    except Exception as e:
        print(f"Error: {e}")
    
    path = os.path.join(output_parquet, 'metrica_historico.parquet')
    df.to_parquet(path, index=False)
    
    return path

def load_to_redshift(**kwargs):
    parquet_to_load = kwargs['ti'].xcom_pull(task_ids='e_metrica')
    redshift_conn_string = kwargs['redshift_conn_string']
    redshift_table = kwargs['redshift_table']
    schema = kwargs['schema']
    
    # Load transformed data from Parquet
    df = pd.read_parquet(parquet_to_load)
    
    # Create SQLAlchemy engine for Redshift
    engine = create_engine(redshift_conn_string)

    # Load data to Redshift table
    try:
        df.to_sql(redshift_table, engine, schema, if_exists='append', index=False, method='multi')
        print(f"Data loaded into Redshift table {redshift_table}")
    except Exception as e:
        print(f"Error al conectar a Redshift: {e}")

# Define DAG
with DAG(
    'el_metrica_historica',
    default_args={
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
    },
    description='Extract metrica_diaria and load into aqi_metrica_historica ',
    schedule_interval='1 0 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    # Task 1: Extract data from aqi_metrica_diaria
    extract_task = PythonOperator(
        task_id='e_metrica',
        python_callable=e_metrica,
        op_kwargs={'redshift_conn_string': REDSHIFT_CONN_STRING,
                   'redshift_metrica': REDSHIFT_METRICA,
                   'output_parquet': DATA_PATH},
    )
    
    # Task 2: Load data into aqi_metrica_historica
    load_task = PythonOperator(
        task_id='load_to_redshift',
        python_callable=load_to_redshift,
        op_kwargs={
            'redshift_table': REDSHIFT_HISTORICA,
            'redshift_conn_string': REDSHIFT_CONN_STRING,
            'schema': SCHEMA
        },
    )
    
    # Set task dependencies
    extract_task >> load_task