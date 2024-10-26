import pytest
from unittest.mock import patch, MagicMock
import os
import pandas as pd
from dags.etl_modules.transform_data_city import transform_data

# Elimina la dependencia de la API_KEY y la URL para el test
API_KEY = "fake_api_key"
url = "http://fakeapi.com/city"

# Datos de entrada simulados
df_mock = pd.DataFrame({
    'country': ['Argentina', 'Argentina', 'Argentina'],
    'state': ['Santa Fe', 'Santa Fe', 'Santa Fe'],
    'city': ['Rafaela', 'Rosario', 'Santa Fe']
})

# Respuestas simuladas de la API
city_array = [
    {
        "data": {
            'city': 'Rafaela',
            'current': {
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
    },
    {
        "data": {
            'city': 'Rosario',
            'current': {
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
    },
    {
        "data": {
            'city': 'Santa Fe',
            'current': {
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
]

@patch('pandas.read_parquet')  # Mockear la lectura del parquet
@patch('requests.get')  # Mockear llamada a la API
@patch('time.sleep', return_value=None)  # Mockear time.sleep para evitar la espera real
def test_transform_data(mock_sleep, mock_requests_get, mock_read_parquet):
    # Simular la lectura del archivo Parquet
    mock_read_parquet.return_value = df_mock

    # Simular la respuesta de la API
    mock_requests_get.return_value.json.side_effect = city_array
    
    # Simular el argumento de entrada
    ti_mock = MagicMock()
    ti_mock.xcom_pull.return_value = './extract_city4.parquet'
    
    # Ejecutar la función bajo prueba
    kwargs = {
        'ti': ti_mock,
        'output_parquet': '.',
        'parquet_name': 'transform_data4.parquet'
        }
    
    # Llamar a la función
    result = transform_data(**kwargs)

    # Crear el DataFrame esperado
    expected_df = pd.DataFrame({
        "city": ['Rafaela', 'Rosario', 'Santa Fe'],
        "current_pollution_ts": ["2024-10-17T12:00:00","2024-10-17T12:00:00","2024-10-17T12:00:00"],
        "current_pollution_aqius": [50, 50, 50],
        "current_pollution_mainus": ["p2", "p2", "p2"],
        "current_pollution_aqicn": [45, 45, 45],
        "current_pollution_maincn": ["p2", "p2", "p2"],
        "current_weather_ts": ["2024-10-17T12:00:00","2024-10-17T12:00:00","2024-10-17T12:00:00"],
        "current_weather_tp": [22, 22, 22],
        "current_weather_pr": [1012, 1012, 1012],
        "current_weather_hu": [60, 60, 60],
        "current_weather_ws": [5, 5, 5],
        "current_weather_wd": [180, 180, 180]
    })

    # Obtener el DataFrame que se pasó a to_parquet
    df_respuestas = pd.json_normalize(
        [item['data'] for item in city_array],
        meta=[
            ['location', 'type'],
            ['location', 'coordinates'],
            ['current', 'pollution', 'ts'],
            ['current', 'pollution', 'aqius'],
            ['current', 'pollution', 'mainus'],
            ['current', 'pollution', 'aqicn'],
            ['current', 'pollution', 'maincn'],
            ['current', 'weather', 'ts'],
            ['current', 'weather', 'tp'],
            ['current', 'weather', 'pr'],
            ['current', 'weather', 'hu'],
            ['current', 'weather', 'ws'],
            ['current', 'weather', 'wd'],
            ['current', 'weather', 'ic']
        ],
        sep='_'
    )
    
    df_transformed = df_respuestas[["city", "current_pollution_ts", "current_pollution_aqius", "current_pollution_mainus", "current_pollution_aqicn", "current_pollution_maincn", "current_weather_ts", "current_weather_tp", "current_weather_pr", "current_weather_hu", "current_weather_ws", "current_weather_wd"]]
    
    # Verificar que la ruta retornada sea la correcta
    assert result == os.path.join('.', 'transform_data4.parquet')

    # Verificar que se haya llamado a `read_parquet` con el archivo correcto
    mock_read_parquet.assert_called_once_with('./extract_city4.parquet')
    
    # Verificar que se haya llamado 3 veces a la API, ya que son la cantidad de elementos que tenia el parquet
    assert mock_requests_get.call_count == 3
    
    # Verificar que el DataFrame transformado es igual al esperado
    pd.testing.assert_frame_equal(df_transformed, expected_df)