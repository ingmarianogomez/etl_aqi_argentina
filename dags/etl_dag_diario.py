import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from etl_modules.et_redshift_diario import et_redshift
from etl_modules.load_to_redshift import load_to_redshift

DATA_PATH = os.path.dirname(os.path.realpath(__file__))
        
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
        task_id='transform_data',
        python_callable=et_redshift,
        op_kwargs={'output_parquet': DATA_PATH},
    )

    # Task 2: Load data into Redshift
    load_task = PythonOperator(
        task_id='load_to_redshift',
        python_callable=load_to_redshift,
        op_kwargs={'redshift_table': 'aqi_info_diaria',
                   'if_exists': 'replace'},
    )

    # Set task dependencies
    extract_task >> load_task