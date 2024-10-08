import os
import pandas as pd
import pytest
from unittest.mock import patch, MagicMock
from dags.etl_dag_metrica_diaria import e_avg


@patch('dags.etl_dag_metrica_diaria.create_engine')  # Parchar la función create_engine
@patch('pandas.read_sql')  # Parchar pandas.read_sql
@patch('pandas.DataFrame.to_parquet')  # Parchar el método to_parquet
def test_e_avg(mock_to_parquet, mock_read_sql, mock_create_engine):
    # Configurar el mock para create_engine
    mock_engine = MagicMock()
    mock_create_engine.return_value = mock_engine

    # Crear un DataFrame simulado para el resultado de la consulta SQL
    mock_df = pd.DataFrame({
        'city': ['Hurlingham', 'Quilmes', 'Saladillo'],
        'avg_3_aqius': [46.0, 44.0, 57.0],
        'avg_3_aqicn': [12.0, 11.0, 18.0],
        'fecha_avg': ['2024-10-07', '2024-10-07', '2024-10-07']
    })

    # Configurar el mock de read_sql para que devuelva el DataFrame simulado
    mock_read_sql.return_value = mock_df

    # Llamar a la función que estás probando
    output_path = e_avg(
        output_parquet='/path/to/output',
        redshift_conn_string='fake_connection_string',
        redshift_historico='fake_table_name'
    )

    # Verificar que el DataFrame se guardó correctamente en un archivo Parquet
    expected_path = os.path.join('/path/to/output', 'avg_3.parquet')
    mock_to_parquet.assert_called_once_with(expected_path, index=False)

    # Comprobar que el resultado de la función es el camino correcto
    assert output_path == expected_path

    # Verificar que la consulta SQL se ejecutó con el engine simulado
    mock_read_sql.assert_called_once_with(
        'SELECT city,AVG(max_aqius)as "avg_3_aqius", AVG(max_aqicn) as "avg_3_aqicn", MAX(fecha) as "fecha_avg" FROM "2024_mariano_gomez_schema".fake_table_name where fecha > (CURRENT_DATE)-4 group by city',
        mock_engine
    )