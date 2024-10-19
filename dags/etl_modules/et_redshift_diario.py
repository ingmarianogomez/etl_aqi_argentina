from sqlalchemy import create_engine, text
import pandas as pd
import os
from config.config import SCHEMA, REDSHIFT_CONN_STRING

redshift_table = 'aqi_weather_data'

def et_redshift(**kwargs):
    output_parquet = kwargs['output_parquet']
    
    engine = create_engine(REDSHIFT_CONN_STRING)
    df = pd.read_sql(f'SELECT city, current_pollution_ts, current_pollution_aqius,current_pollution_aqicn,current_weather_tp, current_weather_pr, current_weather_hu FROM "{SCHEMA}".{redshift_table} WHERE DATE(current_pollution_ts) = (CURRENT_DATE)', engine)
        
    if df is not None and not df.empty:
        df_deduplicated = df.drop_duplicates(subset=['city', 'current_pollution_ts'])
        path = os.path.join(output_parquet, 'data_diaria.parquet')
        df_deduplicated.to_parquet(path)
    else:
        print('Error en la carga de data')
    return path