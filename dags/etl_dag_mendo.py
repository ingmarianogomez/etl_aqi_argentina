from sqlalchemy import create_engine, text
import requests
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
REDSHIFT_TABLE = 'aqi_weather_data'
SCHEMA = "2024_mariano_gomez_schema"

# URL de la API
url = 'http://api.airvisual.com/v2/city'

def extract_city(**kwargs):
    output_parquet = kwargs['output_parquet']
    redshift_conn_string = kwargs['redshift_conn_string']
    
    query = 'select c.city,c.state, c.country from "2024_mariano_gomez_schema".city c where c.state = \'Mendoza\''
    df = pd.read_sql(query, redshift_conn_string)
    
    path = os.path.join(output_parquet, 'extract_city3.parquet')
    df.to_parquet(path, index=False)
    
    return path

def transform_data(**kwargs):
    input_parquet = kwargs['ti'].xcom_pull(task_ids='extract_city')
    output_parquet = kwargs['output_parquet']
    
    df = pd.read_parquet(input_parquet)
    city_array = []
    
    for index, row in df.iterrows():
        country = row['country']
        state = row['state']
        city = row['city']
        
        # Parámetros para la consulta y llamar a la API
        params = {
            'city': city,
            'state': state,
            'country': country,
            'key': quote_plus(os.environ.get("API_KEY"))
        }
        
        response = requests.get(url, params=params)
        api_response = response.json()
        
        # Unificar en un archivo
        city_array.append(api_response)
        
    #convertir a dataframe
    df_respuestas = pd.json_normalize(
        [item['data'] for item in city_array],
        meta=[
            ['location', 'type'],
            ['location', 'coordinates'],
            ['current', 'pollution', 'ts'],
            ['current', 'pollution', 'aqius'],
            ['current', 'pollution', 'mainus'],
            ['current', 'pollution', 'aqicn'],
            ['current', 'pollution', 'maincn'],
            ['current', 'weather', 'ts'],
            ['current', 'weather', 'tp'],
            ['current', 'weather', 'pr'],
            ['current', 'weather', 'hu'],
            ['current', 'weather', 'ws'],
            ['current', 'weather', 'wd'],
            ['current', 'weather', 'ic']
        ],
        sep='_'
    )
    
    df_transformed = df_respuestas[["city", "current_pollution_ts", "current_pollution_aqius", "current_pollution_mainus", "current_pollution_aqicn", "current_pollution_maincn", "current_weather_ts", "current_weather_tp", "current_weather_pr", "current_weather_hu", "current_weather_ws", "current_weather_wd"]]

    path = os.path.join(output_parquet, 'transform_data3.parquet')

    df_transformed.to_parquet(path, index=False)

    print(f"Data transformed and saved to {path}")
    return path

def load_to_redshift(**kwargs):
    transformed_parquet = kwargs['ti'].xcom_pull(task_ids='transform_data')
    redshift_table = kwargs['redshift_table']
    redshift_conn_string = kwargs['redshift_conn_string']
    schema = kwargs['schema']
    
    df = pd.read_parquet(transformed_parquet)
    
    engine = create_engine(redshift_conn_string)

    df.to_sql(redshift_table, engine, schema, if_exists='append', index=False, method='multi')
    print(f"Data loaded into Redshift table {redshift_table}")
        
# Define DAG
with DAG(
    'etl_dag_mendo',
    default_args={
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
    },
    description='ETL pipeline to extract, transform, and load data into Redshift from API AQI',
    schedule_interval='11 * * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    # Task 1: Extract data
    extract_task = PythonOperator(
        task_id='extract_city',
        python_callable=extract_city,
        op_kwargs={'output_parquet': DATA_PATH,
                   'redshift_conn_string': REDSHIFT_CONN_STRING},
    )

    # Task 2: Transform data
    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        op_kwargs={'output_parquet': DATA_PATH},
    )

    # Task 3: Load data into Redshift
    load_task = PythonOperator(
        task_id='load_to_redshift',
        python_callable=load_to_redshift,
        op_kwargs={
            'redshift_table': REDSHIFT_TABLE,
            'redshift_conn_string': REDSHIFT_CONN_STRING,
            'schema': SCHEMA
        },
    )

    # Set task dependencies
    extract_task >> transform_task >> load_task