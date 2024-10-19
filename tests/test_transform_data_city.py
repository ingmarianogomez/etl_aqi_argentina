import pytest
from unittest.mock import patch, MagicMock
import os
import pandas as pd
from dags.etl_modules.transform_data_city import transform_data

# Elimina la dependencia de la API_KEY y la URL para el test
API_KEY = "fake_api_key"
url = "http://fakeapi.com/city"

@patch('requests.get')  # Mockear llamada a la API
@patch('pandas.read_parquet')  # Mockear lectura del parquet
@patch('os.path.join')  # Mockear la creación de la ruta del archivo
@patch.object(pd.DataFrame, 'to_parquet')  # Mockear la escritura a parquet
def test_transform_data(mock_to_parquet, mock_path_join, mock_read_parquet, mock_requests_get):#Los mock pasados como parametro se pasan en forma inversa a como fueron creados
    
    # Simular los datos leídos del archivo Parquet
    df_mock = pd.DataFrame({
        'country': ['Argentina', 'Argentina', 'Argentina'],
        'state': ['Santa Fe', 'Santa Fe', 'Santa Fe'],
        'city': ['Rafaela', 'Rosario', 'Santa Fe']
    })
    mock_read_parquet.return_value = df_mock

    # Simular la respuesta de la API
    api_response = {
        "data": {
            'city': 'Rafaela',
            'state': 'Santa Fe',
            'country': 'Argentina',
            'location': {
                'type': 'Point',
                'coordinates': [10.0, 20.0]
            },
            "current": {
                "pollution": {
                    "ts": "2024-10-17T12:00:00",
                    "aqius": 50,
                    "mainus": "p2",
                    "aqicn": 45,
                    "maincn": "p2"
                },
                "weather": {
                    "ts": "2024-10-17T12:00:00",
                    "tp": 22,
                    "pr": 1012,
                    "hu": 60,
                    "ws": 5,
                    "wd": 180,
                    'ic': 'clear'
                }
            }
        }
    }

    mock_requests_get.return_value.json.return_value = api_response
    
    # Simular la unión de la ruta
    mock_path_join.return_value = os.path.join('.', 'transform_data4.parquet')
    
    # Simular los argumentos de entrada
    ti_mock = MagicMock()
    ti_mock.xcom_pull.return_value = './extract_city4.parquet'

    # Ejecutar la función bajo prueba
    kwargs = {
        'ti': ti_mock,
        'output_parquet': '.',
        'parquet_name': 'transform_data4.parquet'
        }
    
    result = transform_data(**kwargs)

    # Verificar que la ruta retornada sea la correcta
    assert result == os.path.join('.', 'transform_data4.parquet')

    # Verificar que se haya llamado a `read_parquet` con el archivo correcto
    mock_read_parquet.assert_called_once_with('./extract_city4.parquet')
    
    # Verificar que se haya llamado 3 veces a la API, ya que son la cantidad de elementos que tenia el parquet
    assert mock_requests_get.call_count == 3

    # Verificar que `to_parquet` fue llamado una vez con el DataFrame correcto
    mock_to_parquet.assert_called_once_with(os.path.join('.', 'transform_data4.parquet'), index=False)
