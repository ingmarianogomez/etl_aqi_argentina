# ETL - AQI - Argentina 

## Introducción

El proyecto es un ETL que se comporta de la siguiente forma:

- Extract (E): Se obtiene informacion específica de una ciudad, con su contaminación y clima actual, consultando una API de la siguiente forma 
 
```https
  GET api.airvisual.com/v2/city?
```

| Parameter | Type     | Description                |
| :-------- | :------- | :------------------------- |
| `api_key` | `string` | **Required**. Your API key |
| `city` | `string` | **Required**. Your API key |
| `state` | `string` | **Required**. Your API key |
| `country` | `string` | **Required**. Your API key |

- Transform (T): La información extraida es transformada mediante funciones de Pandas y query SQL, utilizando archivos parquet para almacenar archivos intermedios.

- Load (L): La informacion transformada es persistida en una base de datos redshift.


## Ejecutar Airflow localmente :computer:

#### Requisitos
 - Tener instalado Python 3.10.12 

#### Implementacion

Desde la terminal clonar el repositorio

```bash
  git clone https://github.com/ingmarianogomez/etl_aqi_argentina.git
```

Ingresar al directorio del proyecto

```bash
  cd etl_aqi_argentina
```

Crear un entorno virtual

```bash
  python -m venv venv
  source venv/bin/activate
```

Establecer como variables de entorno las contraseñas compartidas por privado

`API_KEY`
`REDSHIFT_PASSWORD`

Instalar las dependencias

```bash
  pip install -r requirements.txt
```

Para iniciar la base de datos, configurar el entorno y arrancar tanto el servidor web como el scheduler ejecutar

```bash
   AIRFLOW_HOME=$(pwd) airflow standalone
```

## Ejecutar Airflow mediante Docker :whale:

#### Requisitos
 - Tener instalado Docker Desktop 

#### Implementacion

Desde la terminal clonar el repositorio

```bash
  git clone https://github.com/ingmarianogomez/etl_aqi_argentina.git
```

Ingresar al directorio del proyecto

```bash
  cd etl_aqi_argentina
```

Almacenar en una variable el ID de usuario actual y creación de carpetas necesarias

```bash
  mkdir -p ./logs ./plugins ./config
  echo -e "AIRFLOW_UID=$(id -u)" > .env
```
En el archivo .env creado se deberan agregar la `API_KEY` y la `REDSHIFT_PASSWORD`


Inicio de la base de datos

```bash
  docker compose up airflow-init
```

Correr Airflow

```bash
  docker compose up
```
