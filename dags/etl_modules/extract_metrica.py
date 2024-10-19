from sqlalchemy import create_engine, text
import pandas as pd
import os
from config.config import SCHEMA, REDSHIFT_CONN_STRING


def e_metrica(**kwargs):
    output_parquet = kwargs['output_parquet']
    redshift_aqi_info_diaria = kwargs['redshift_aqi_info_diaria']
    parquet_name = kwargs['parquet_name']
    
    engine = create_engine(REDSHIFT_CONN_STRING)
    df = pd.read_sql(f'SELECT city, DATE(current_pollution_ts) as "fecha", MAX(current_pollution_aqius) as "max_aqius",MAX(current_pollution_aqicn) as "max_aqicn", AVG(current_weather_tp) as "avg_temp",MAX(current_weather_tp) as "max_temp",MAX(current_weather_pr) as "max_pre",MAX(current_weather_hu) as "max_hum" FROM "{SCHEMA}".{redshift_aqi_info_diaria} d group by city, DATE(current_pollution_ts)', engine)
    
    path = os.path.join(output_parquet, parquet_name)
    df.to_parquet(path, index=False)
    
    return path