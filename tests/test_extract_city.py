import os
import pandas as pd
import pytest
from unittest.mock import patch, MagicMock
from dags.etl_modules.extract_city import extract_city

@patch('pandas.read_sql')
@patch('os.path.join')
@patch.object(pd.DataFrame, 'to_parquet')
def test_extract_city(mock_to_parquet, mock_path_join, mock_read_sql): #Los mock pasados como parametro se pasan en forma inversa a como fueron creados
    output_parquet = '.'  
    state = 'Santa Fe'  
    parquet_name = 'extract_city4.parquet'  

    # Dataframe que simulara la salida del SQL
    expected_data = {
        'country': ['Argentina', 'Argentina', 'Argentina'],
        'state': ['Santa Fe', 'Santa Fe', 'Santa Fe'],
        'city': ['Rafaela', 'Rosario', 'Santa Fe']
    }
    expected_df = pd.DataFrame(expected_data)

    # Mockear los valores de retorno de las funciones
    mock_read_sql.return_value = expected_df
    mock_path_join.return_value = os.path.join(output_parquet, parquet_name)

    # Ejecutamos la funcion y guardamos en result en el return
    result = extract_city(output_parquet=output_parquet, state=state, parquet_name=parquet_name)

    # Verificar que la función devolvió la ruta correcta
    assert result == os.path.join(output_parquet, parquet_name)

    # Verificar que to_parquet fue llamado correctamente
    mock_to_parquet.assert_called_once_with(os.path.join(output_parquet, parquet_name), index=False)