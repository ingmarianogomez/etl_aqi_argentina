import requests
import pandas as pd
import os
from dags.config.config import url, API_KEY


def transform_data(**kwargs):
    input_parquet = kwargs['ti'].xcom_pull(task_ids='extract_city')
    output_parquet = kwargs['output_parquet']
    parquet_name = kwargs['parquet_name']
    
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
            'key': API_KEY
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

    path = os.path.join(output_parquet, parquet_name)

    df_transformed.to_parquet(path, index=False)

    print(f"Data transformed and saved to {path}")
    return path