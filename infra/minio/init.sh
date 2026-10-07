#!/bin/sh
# Inicializa MinIO para el pipeline:
#  1. Crea los buckets (raw = zona de aterrizaje, reportes = capa de consumo).
#  2. Aplica lifecycle policies (los datos en cuarentena caducan a los 30 días,
#     los crudos a los 365 días: la fuente de verdad histórica es Snowflake).
#  3. Crea el usuario de Airflow con una política de MÍNIMO privilegio
#     (sin DeleteObject ni acceso a otros buckets).
set -eu

mc alias set local http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"

mc mb --ignore-existing local/deporte-raw
mc mb --ignore-existing local/deporte-reportes
mc version enable local/deporte-raw

# Se recrean las reglas en cada arranque para que el script sea idempotente.
mc ilm rule rm --all --force local/deporte-raw >/dev/null 2>&1 || true
mc ilm rule add local/deporte-raw --prefix "cuarentena/" --expire-days 30
mc ilm rule add local/deporte-raw --prefix "api/" --expire-days 365
mc ilm rule add local/deporte-raw --prefix "gps/crudo/" --expire-days 365
mc ilm rule add local/deporte-raw --prefix "mysql/" --expire-days 365

mc admin policy create local politica-airflow-deporte /politica_airflow.json
mc admin user add local "$MINIO_AIRFLOW_ACCESS_KEY" "$MINIO_AIRFLOW_SECRET_KEY"
mc admin policy attach local politica-airflow-deporte --user "$MINIO_AIRFLOW_ACCESS_KEY" || true

echo "MinIO listo: buckets, lifecycle y usuario de mínimo privilegio configurados."
