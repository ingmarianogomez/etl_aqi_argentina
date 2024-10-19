import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from etl_modules.extract_avg import e_avg
from etl_modules.extract_metrica import e_metrica
from etl_modules.transform_data_metrica import transform_data
from etl_modules.load_to_redshift import load_to_redshift

DATA_PATH = os.path.dirname(os.path.realpath(__file__))
        
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
                   'parquet_name': 'avg_3.parquet',
                   'redshift_historico': 'aqi_metrica_historica'},
    )

    # Task 2: Extract data from aqi_info_diaria
    extract_task2 = PythonOperator(
        task_id='e_metrica',
        python_callable=e_metrica,
        op_kwargs={'output_parquet': DATA_PATH,
                   'parquet_name': 'metrica_diaria.parquet', 
                   'redshift_aqi_info_diaria': 'aqi_info_diaria'},
    )

    # Task 3: Transform data
    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        op_kwargs={'output_parquet': DATA_PATH,
                   'parquet_name': 'metrica_completa.parquet'},
    )

    # Task 4: Load data into Redshift
    load_task = PythonOperator(
        task_id='load_to_redshift',
        python_callable=load_to_redshift,
        op_kwargs={'redshift_table': 'aqi_metrica_diaria',
                   'if_exists': 'replace'},
    )

    # Set task dependencies
    extract_task >> extract_task2 >> transform_task >> load_task