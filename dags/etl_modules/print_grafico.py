from sqlalchemy import create_engine, text
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from config.config import SCHEMA, REDSHIFT_CONN_STRING

redshift_aqi_info_diaria = "aqi_info_diaria"

def print_grafico(**kwargs):
    pais_indice = kwargs['pais_indice']
    campo_pais_indice = kwargs['campo_pais_indice']
    
    engine = create_engine(REDSHIFT_CONN_STRING)
    df = pd.read_sql(f'SELECT city, current_pollution_ts, {campo_pais_indice} FROM "{SCHEMA}".{redshift_aqi_info_diaria} d', engine)

    # Visualizar los datos
    plt.figure(figsize=(12, 6))

    # Graficar AQI de EE. UU. y AQI mundial para cada ciudad
    for city in df['city'].unique():
        city_data = df[df['city'] == city]
        plt.plot(city_data['current_pollution_ts'], city_data[f'{campo_pais_indice}'], marker='o', label=f'{pais_indice} - {city}')

    plt.title(f'Calidad del Aire ({pais_indice}) a lo Largo del Día')
    plt.xlabel('Hora')
    plt.ylabel(f'Índice de Calidad del Aire ({pais_indice})')
    plt.xticks(rotation=45)
    plt.legend()
    plt.tight_layout()
    plt.grid()

    # Guardar el gráfico como un archivo PNG
    plt.savefig(f'{pais_indice}_plot.png')