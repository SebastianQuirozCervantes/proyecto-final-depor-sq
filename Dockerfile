FROM apache/airflow:2.10.5-python3.11

ARG AIRFLOW_VERSION=2.10.5
ARG PYTHON_VERSION=3.11

# 1) Dependencias de Airflow (Cosmos) respetando las constraints oficiales.
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# 2) dbt en un entorno virtual aislado: evita conflictos de dependencias entre
#    dbt y Airflow (patrón recomendado por Astronomer Cosmos).
COPY requirements-dbt.txt /requirements-dbt.txt
RUN python -m venv /opt/airflow/dbt_venv \
 && /opt/airflow/dbt_venv/bin/pip install --no-cache-dir -r /requirements-dbt.txt

ENV PYTHONPATH=/opt/airflow
