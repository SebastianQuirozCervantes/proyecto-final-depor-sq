#!/bin/bash
# Crea el usuario que usa Airflow con el MÍNIMO privilegio necesario:
# solo SELECT sobre la tabla de ventas. Las credenciales llegan por variables
# de entorno desde el archivo .env (nunca escritas en el repositorio).
set -euo pipefail
mysql -uroot -p"${MYSQL_ROOT_PASSWORD}" <<SQL
CREATE USER IF NOT EXISTS '${MYSQL_READER_USER}'@'%' IDENTIFIED BY '${MYSQL_READER_PASSWORD}';
GRANT SELECT ON taquilla.ventas_entradas TO '${MYSQL_READER_USER}'@'%';
FLUSH PRIVILEGES;
SQL
echo "Usuario de solo lectura ${MYSQL_READER_USER} creado."
