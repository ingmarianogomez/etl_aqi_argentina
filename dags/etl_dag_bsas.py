import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from etl_modules.extract_city import extract_city
from etl_modules.transform_data_city import transform_data
from etl_modules.load_to_redshift import load_to_redshift

DATA_PATH = os.path.dirname(os.path.realpath(__file__))
        
# Define DAG
with DAG(
    'etl_dag_bsas',
    default_args={
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
    },
    description='ETL pipeline to extract, transform, and load data into Redshift from API AQI',
    schedule_interval='8 * * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    # Task 1: Extract data
    extract_task = PythonOperator(
        task_id='extract_city',
        python_callable=extract_city,
        op_kwargs={'output_parquet': DATA_PATH,
                   'state': "Buenos Aires",
                   'parquet_name': 'extract_city2.parquet'},
    )

    # Task 2: Transform data
    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        op_kwargs={'output_parquet': DATA_PATH,
                   'parquet_name': 'transform_data2.parquet'},
    )

    # Task 3: Load data into Redshift
    load_task = PythonOperator(
        task_id='load_to_redshift',
        python_callable=load_to_redshift,
        op_kwargs={'redshift_table': 'aqi_weather_data',
                   'if_exists': 'append'},
    )

    # Set task dependencies
    extract_task >> transform_task >> load_task