import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from etl_modules.extract_met_avg import e_metrica
from etl_modules.load_to_redshift import load_to_redshift

DATA_PATH = os.path.dirname(os.path.realpath(__file__))

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
        task_id='transform_data',
        python_callable=e_metrica,
        op_kwargs={'redshift_metrica': 'aqi_metrica_diaria',
                   'output_parquet': DATA_PATH,
                   'parquet_name': 'metrica_historico.parquet'},
    )
    
    # Task 2: Load data into aqi_metrica_historica
    load_task = PythonOperator(
        task_id='load_to_redshift',
        python_callable=load_to_redshift,
        op_kwargs={'redshift_table': 'aqi_metrica_historica',
                   'if_exists': 'append'},
    )
    
    # Set task dependencies
    extract_task >> load_task