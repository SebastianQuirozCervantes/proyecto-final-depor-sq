-- =====================================================================
-- DeporData · Operación diaria en Snowflake (Clase 7): Time Travel y
-- Zero-Copy Cloning aplicados al caso del club.
-- =====================================================================
USE ROLE DEPORTE_PIPELINE_ROLE;
USE WAREHOUSE DEPORTE_WH;
USE DATABASE DEPORTE_DB;

-- 1) Time Travel: ¿cómo estaba el semáforo de riesgo hace 1 hora?
--    Útil si una corrida de dbt dejó el mart con datos incorrectos.
SELECT nombre, acwr, nivel_riesgo
FROM MARTS.MART_RIESGO_LESION AT(OFFSET => -60*60)
ORDER BY acwr DESC;

-- 2) Recuperar una tabla borrada por error.
-- DROP TABLE MARTS.FCT_RENDIMIENTO_PARTIDO;
-- UNDROP TABLE MARTS.FCT_RENDIMIENTO_PARTIDO;

-- 3) Zero-Copy Cloning: entorno de pruebas idéntico a producción, en
--    segundos y sin duplicar almacenamiento. Ideal para probar un cambio de
--    dbt (por ejemplo, otro umbral de ACWR) antes de mezclarlo a main.
CREATE OR REPLACE SCHEMA MARTS_PRUEBA CLONE MARTS;

-- 4) Auditoría de lotes cargados por Airflow (linaje en RAW).
SELECT _LOTE_ID, COUNT(*) AS filas, MAX(_CARGADO_EN) AS cargado_en
FROM RAW.GPS_CARGA_FISICA
GROUP BY _LOTE_ID
ORDER BY cargado_en DESC
LIMIT 10;

-- 5) Costo: créditos consumidos por el warehouse del pipeline (últimos 7 días).
SELECT DATE_TRUNC('day', start_time) AS dia, SUM(credits_used) AS creditos
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE warehouse_name = 'DEPORTE_WH' AND start_time > DATEADD(day, -7, CURRENT_TIMESTAMP())
GROUP BY 1 ORDER BY 1;
