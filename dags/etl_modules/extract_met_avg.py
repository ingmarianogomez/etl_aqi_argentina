from sqlalchemy import create_engine, text
import pandas as pd
import os
from config.config import SCHEMA, REDSHIFT_CONN_STRING


def e_metrica(**kwargs):
    redshift_metrica = kwargs['redshift_metrica']
    output_parquet = kwargs['output_parquet']
    parquet_name = kwargs['parquet_name']
    
    engine = create_engine(REDSHIFT_CONN_STRING)
    df = pd.read_sql(f'SELECT * FROM "{SCHEMA}".{redshift_metrica}', engine)
    
    path = os.path.join(output_parquet, parquet_name)
    df.to_parquet(path, index=False)
    
    return path

