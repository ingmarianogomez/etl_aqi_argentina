import pandas as pd
import os

def transform_data(**kwargs):
    input_parquet_diaria = kwargs['ti'].xcom_pull(task_ids='e_metrica')
    input_parquet_avg = kwargs['ti'].xcom_pull(task_ids='e_avg')
    output_parquet = kwargs['output_parquet']
    parquet_name = kwargs['parquet_name']
    
    df_diaria = pd.read_parquet(input_parquet_diaria)
    df_avg = pd.read_parquet(input_parquet_avg)
    df_join = pd.merge(df_diaria, df_avg, on='city', how='outer')

    path = os.path.join(output_parquet, parquet_name)

    df_join.to_parquet(path, index=False)

    print(f"Data transformed and saved to {path}")
    return path