import os
from urllib.parse import quote_plus

# DB CONNECTIONS
user='2024_mariano_gomez'
password = quote_plus(os.environ.get("REDSHIFT_PASSWORD"))
host='redshift-pda-cluster.cnuimntownzt.us-east-2.redshift.amazonaws.com'
port='5439'
dbname='pda'

REDSHIFT_CONN_STRING = f'redshift+psycopg2://{user}:{password}@{host}:{port}/{dbname}'
SCHEMA = "2024_mariano_gomez_schema"


# API AQI
url = 'http://api.airvisual.com/v2/city'
API_KEY = quote_plus(os.environ.get("API_KEY"))