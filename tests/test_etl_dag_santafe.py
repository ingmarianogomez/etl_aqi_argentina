import os
import pandas as pd
import pytest
from unittest.mock import patch, MagicMock
from dags.etl_dag_sanfe import transform_data

# Simula las variables de entorno necesarias
os.environ["API_KEY"] = "falsa_api_key"
url = 'http://api.airvisual.com'

def test_transform_data():
    # Configurar mocks
    with patch('pandas.read_parquet') as mock_read_parquet, \
         patch('requests.get') as mock_get, \
         patch('pandas.DataFrame.to_parquet') as mock_to_parquet:

        # Simular el DataFrame de entrada
        mock_read_parquet.return_value = pd.DataFrame({
            'country': ['Argentina', 'Argentina', 'Argentina'],
            'state': ['Santa Fe', 'Santa Fe', 'Santa Fe'],
            'city': ['Rafaela', 'Rosario', 'Santa Fe']
        })

        # Simular respuesta de la API
        mock_get.return_value = MagicMock(status_code=200)
        mock_get.return_value.json.return_value = {
            'data': {
                'city': 'Rafaela',
                'state': 'Santa Fe',
                'country': 'Argentina',
                'location': {
                    'type': 'Point',
                    'coordinates': [10.0, 20.0]
                },
                'current': {
                    'pollution': {
                        'ts': '2024-10-04T00:00:00Z',
                        'aqius': 50,
                        'mainus': 'PM2.5',
                        'aqicn': 30,
                        'maincn': 'PM10'
                    },
                    'weather': {
                        'ts': '2024-10-04T00:00:00Z',
                        'tp': 20,
                        'pr': 1010,
                        'hu': 60,
                        'ws': 5,
                        'wd': 180,
                        'ic': 'clear'
                    }
                }
            }
        }

        # Llamar a la función con el parametro de la tarea anterior y guardar el return en una variable
        
        output_path = transform_data(ti=MagicMock(xcom_pull=lambda task_ids: 'extract_city4.parquet'),
                                      output_parquet='/path/to/return')

        # Probar que se lee el parquet
        mock_read_parquet.assert_called_once_with('extract_city4.parquet')

        # Verificar que se haya llamado 3 veces a la API, ya que son la cantidad de elementos que tenia el parquet
        assert mock_get.call_count == 3 

        # Verificar que se llamo una vez el to_parquet
        mock_to_parquet.assert_called_once_with('/path/to/return/transform_data4.parquet', index=False)

        # Verificar el return de la funcion
        assert output_path == '/path/to/return/transform_data4.parquet'