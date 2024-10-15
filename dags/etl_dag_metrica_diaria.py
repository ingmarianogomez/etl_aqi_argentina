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
REDSHIFT_TABLE_CONSULTA_DIARIA = 'aqi_info_diaria'
REDSHIFT_TABLE_CONSULTA_AVG = 'aqi_metrica_historica'
REDSHIFT_TABLE_SALIDA = 'aqi_metrica_diaria'
SCHEMA = "2024_mariano_gomez_schema"

def e_avg(**kwargs):
    output_parquet = kwargs['output_parquet']
    redshift_conn_string = kwargs['redshift_conn_string']
    redshift_historico = kwargs['redshift_historico']
    
    engine = create_engine(redshift_conn_string)
    df = pd.read_sql(f'SELECT city,AVG(max_aqius)as "avg_3_aqius", AVG(max_aqicn) as "avg_3_aqicn", MAX(fecha) as "fecha_avg" FROM "2024_mariano_gomez_schema".{redshift_historico} where fecha > (CURRENT_DATE)-4 group by city', engine)
    
    path = os.path.join(output_parquet, 'avg_3.parquet')
    df.to_parquet(path, index=False)
    
    return path

def e_metrica(**kwargs):
    output_parquet = kwargs['output_parquet']
    redshift_conn_string = kwargs['redshift_conn_string']
    redshift_aqi_info_diaria = kwargs['redshift_aqi_info_diaria']
    
    engine = create_engine(redshift_conn_string)
    df = pd.read_sql(f'SELECT city, DATE(current_pollution_ts) as "fecha", MAX(current_pollution_aqius) as "max_aqius",MAX(current_pollution_aqicn) as "max_aqicn", AVG(current_weather_tp) as "avg_temp",MAX(current_weather_tp) as "max_temp",MAX(current_weather_pr) as "max_pre",MAX(current_weather_hu) as "max_hum" FROM "2024_mariano_gomez_schema".{redshift_aqi_info_diaria} d group by city, DATE(current_pollution_ts)', engine)
    
    path = os.path.join(output_parquet, 'metrica_diaria.parquet')
    df.to_parquet(path, index=False)
    
    return path

def transform_data(**kwargs):
    input_parquet_diaria = kwargs['ti'].xcom_pull(task_ids='e_metrica')
    input_parquet_avg = kwargs['ti'].xcom_pull(task_ids='e_avg')
    output_parquet = kwargs['output_parquet']
    
    df_diaria = pd.read_parquet(input_parquet_diaria)
    df_avg = pd.read_parquet(input_parquet_avg)
    df_join = pd.merge(df_diaria, df_avg, on='city', how='outer')

    path = os.path.join(output_parquet, 'metrica_completa.parquet')

    df_join.to_parquet(path, index=False)

    print(f"Data transformed and saved to {path}")
    return path

def load_to_redshift(**kwargs):
    transformed_parquet = kwargs['ti'].xcom_pull(task_ids='transform_data')
    redshift_table = kwargs['redshift_table']
    redshift_conn_string = kwargs['redshift_conn_string']
    schema = kwargs['schema']
    
    # Load transformed data from Parquet
    df = pd.read_parquet(transformed_parquet)
    
    # Create SQLAlchemy engine for Redshift
    engine = create_engine(redshift_conn_string)

    # Load data to Redshift table
    df.to_sql(redshift_table, engine, schema, if_exists='replace', index=False, method='multi')
    print(f"Data loaded into Redshift table {redshift_table}")
        
# Define DAG
with DAG(
    'etl_metrica_diaria_avg',
    default_args={
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
    },
    description='Extract info_diaria, transform with avg and load to redshift metricas',
    schedule_interval='40 * * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:
    
    # Task 1: Extract data from aqi_metrica_historica
    extract_task = PythonOperator(
        task_id='e_avg',
        python_callable=e_avg,
        op_kwargs={'output_parquet': DATA_PATH,
                   'redshift_conn_string': REDSHIFT_CONN_STRING,
                   'redshift_historico': REDSHIFT_TABLE_CONSULTA_AVG},
    )

    # Task 2: Extract data from aqi_info_diaria
    extract_task2 = PythonOperator(
        task_id='e_metrica',
        python_callable=e_metrica,
        op_kwargs={'output_parquet': DATA_PATH,
                   'redshift_conn_string': REDSHIFT_CONN_STRING,
                   'redshift_aqi_info_diaria': REDSHIFT_TABLE_CONSULTA_DIARIA},
    )

    # Task 3: Transform data
    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        op_kwargs={'output_parquet': DATA_PATH},
    )

    # Task 4: Load data into Redshift
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
    extract_task >> extract_task2 >> transform_task >> load_task