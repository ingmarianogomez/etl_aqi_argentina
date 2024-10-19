from sqlalchemy import create_engine, text
import pandas as pd
from config.config import REDSHIFT_CONN_STRING, SCHEMA

def load_to_redshift(**kwargs):
    transformed_parquet = kwargs['ti'].xcom_pull(task_ids='transform_data')
    redshift_table = kwargs['redshift_table']
    if_exists = kwargs['if_exists']
    
    df = pd.read_parquet(transformed_parquet)
    
    engine = create_engine(REDSHIFT_CONN_STRING)

    df.to_sql(redshift_table, engine, SCHEMA, if_exists= if_exists, index=False, method='multi')
    print(f"Data loaded into Redshift table {redshift_table}")