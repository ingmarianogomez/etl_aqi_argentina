from sqlalchemy import create_engine, text
import pandas as pd
import os
from config.config import SCHEMA, REDSHIFT_CONN_STRING


def e_avg(**kwargs):
    output_parquet = kwargs['output_parquet']
    redshift_historico = kwargs['redshift_historico']
    parquet_name = kwargs['parquet_name']
    
    engine = create_engine(REDSHIFT_CONN_STRING)
    df = pd.read_sql(f'SELECT city,AVG(max_aqius)as "avg_3_aqius", AVG(max_aqicn) as "avg_3_aqicn", MAX(fecha) as "fecha_avg" FROM "{SCHEMA}".{redshift_historico} where fecha > (CURRENT_DATE)-4 group by city', engine)
    
    path = os.path.join(output_parquet, parquet_name)
    df.to_parquet(path, index=False)
    
    return path