from sqlalchemy import create_engine, text
import requests
import pandas as pd
import os
from urllib.parse import quote_plus

user='2024_mariano_gomez'
password = quote_plus(os.environ.get("REDSHIFT_PASSWORD"))
host='redshift-pda-cluster.cnuimntownzt.us-east-2.redshift.amazonaws.com'
port='5439'
dbname='pda'

REDSHIFT_CONN_STRING = f'redshift+psycopg2://{user}:{password}@{host}:{port}/{dbname}'

# URL de la API
url = 'http://api.airvisual.com/v2/city'

def extract_city(output_parquet:str, redshift_conn_string: str):

    query = 'select c.city,c.state, c.country from "2024_mariano_gomez_schema".city c where c.state = \'La Pampa\''
    df = pd.read_sql(query, redshift_conn_string)
    
    path = os.path.join(output_parquet, 'extract_city.parquet')
    df.to_parquet(path, index=False)

def et_data(input_parquet:str, output_parquet:str):
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
        api_response = response.json()  # Ajusta según el formato de respuesta
        
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

    path = os.path.join(output_parquet, 'transform_data.parquet')

    # Save the transformed data to another Parquet file
    df_transformed.to_parquet(path, index=False)

    print(f"Data transformed and saved to {path}")
    return path

def load_to_redshift(transformed_parquet: str, redshift_table: str, redshift_conn_string: str, schema: str = "2024_mariano_gomez_schema"):    
    # Load transformed data from Parquet
    df = pd.read_parquet(transformed_parquet)
    
    # Create SQLAlchemy engine for Redshift
    engine = create_engine(redshift_conn_string)

    # Load data to Redshift table
    try:
        df.to_sql(redshift_table, engine, schema, if_exists='append', index=False, method='multi')
        print(f"Data loaded into Redshift table {redshift_table}")
    except Exception as e:
        print(f"Error al conectar a Redshift: {e}")