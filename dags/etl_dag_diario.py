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
REDSHIFT_TABLE_CONSULTA = 'aqi_weather_data'
REDSHIFT_TABLE_SALIDA = 'aqi_info_diaria'
SCHEMA = "2024_mariano_gomez_schema"


def et_redshift(**kwargs):
    output_parquet = kwargs['output_parquet']
    redshift_aqi_info_diaria = kwargs['aqi_info_diaria']
    redshift_conn_string = kwargs['redshift_conn_string']
    try:
        engine = create_engine(redshift_conn_string)
        df = pd.read_sql(f'SELECT city, current_pollution_ts, current_pollution_aqius,current_pollution_aqicn,current_weather_tp, current_weather_pr, current_weather_hu FROM "2024_mariano_gomez_schema".{redshift_aqi_info_diaria} WHERE DATE(current_pollution_ts) = (CURRENT_DATE)', engine)
    except Exception as e:
        print(f"Error: {e}")
        
    if df is not None and not df.empty:
        df_deduplicated = df.drop_duplicates(subset=['city', 'current_pollution_ts'])
        path = os.path.join(output_parquet, 'data_diaria.parquet')
        df_deduplicated.to_parquet(path)
    else:
        print('Error en la carga de data')
    return path

def load_to_redshift(**kwargs):
    transformed_parquet = kwargs['ti'].xcom_pull(task_ids='et_redshift')
    redshift_table = kwargs['redshift_table']
    redshift_conn_string = kwargs['redshift_conn_string']
    schema = kwargs['schema']
    
    # Load transformed data from Parquet
    df = pd.read_parquet(transformed_parquet)
    
    # Create SQLAlchemy engine for Redshift
    engine = create_engine(redshift_conn_string)

    # Load data to Redshift table
    try:
        df.to_sql(redshift_table, engine, schema, if_exists='replace', index=False, method='multi')
        print(f"Data loaded into Redshift table {redshift_table}")
    except Exception as e:
        print(f"Error al conectar a Redshift: {e}")
        
# Define DAG
with DAG(
    'etl_dag_diario',
    default_args={
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
    },
    description='ETL pipeline to extract, transform, and load data into Redshift info_diaria from weather_data',
    schedule_interval='30 * * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    # Task 1: Extract data
    extract_task = PythonOperator(
        task_id='et_redshift',
        python_callable=et_redshift,
        op_kwargs={'output_parquet': DATA_PATH,
                   'redshift_conn_string': REDSHIFT_CONN_STRING,
                   'aqi_info_diaria': REDSHIFT_TABLE_CONSULTA
                   },
    )

    # Task 2: Load data into Redshift
    load_task = PythonOperator(
        task_id='load_to_redshift',
        python_callable=load_to_redshift,
        op_kwargs={
            'redshift_table': REDSHIFT_TABLE_SALIDA,
            'redshift_conn_string': REDSHIFT_CONN_STRING,
            'schema': SCHEMA
        },
    )

    # Set task dependencies
    extract_task >> load_task