import pandas as pd
import os
from dags.config.config import SCHEMA, REDSHIFT_CONN_STRING


def extract_city(**kwargs):
    output_parquet = kwargs['output_parquet']
    state = kwargs['state']
    parquet_name = kwargs['parquet_name']
    
    query = f'select c.city,c.state, c.country from "{SCHEMA}".city c where c.state = \'{state}\''
    df = pd.read_sql(query, REDSHIFT_CONN_STRING)
    
    path = os.path.join(output_parquet, parquet_name)
    df.to_parquet(path, index=False)
    
    return path