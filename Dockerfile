# FROM apache/airflow:2.10.2

# USER root
# COPY requirements.txt .

# USER airflow
# RUN pip install --no-cache-dir -r requirements.txt

# USER airflow

FROM apache/airflow:2.10.2

# Cambia a root para instalar dependencias del sistema
USER root

# Instala gcc y otras dependencias necesarias
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    make \
    python3-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Cambia al usuario airflow para la instalación de Python
USER airflow

# Copia el archivo de requisitos
COPY requirements.txt .

# Instala las dependencias de Python
RUN pip install --no-cache-dir -r requirements.txt