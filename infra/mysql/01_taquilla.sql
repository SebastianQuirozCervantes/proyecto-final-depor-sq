-- =====================================================================
-- Base de datos HEREDADA del sistema de taquilla del Club Deportivo SQ.
-- Simula el sistema on-premise de venta de entradas (fuente 3 del pipeline).
--
-- Regla compartida con el calendario de los simuladores:
--   el club juega de local en la Liga Andina cada 14 días desde 2026-08-01
--   (jornadas impares). Cada partido de local tiene ventas durante los 7 días
--   previos, por tribuna y canal. Algunas ventas se anulan un día después:
--   esa MODIFICACIÓN es la que la extracción incremental por updated_at debe
--   capturar.
-- =====================================================================
CREATE DATABASE IF NOT EXISTS taquilla;
USE taquilla;

CREATE TABLE IF NOT EXISTS ventas_entradas (
    venta_id            BIGINT       NOT NULL AUTO_INCREMENT PRIMARY KEY,
    fecha_partido       DATE         NOT NULL,
    codigo_competicion  CHAR(3)      NOT NULL,
    tribuna             VARCHAR(20)  NOT NULL,
    canal               VARCHAR(20)  NOT NULL,
    cantidad            INT          NOT NULL,
    precio_unitario     DECIMAL(8,2) NOT NULL,
    estado              VARCHAR(12)  NOT NULL,
    created_at          DATETIME     NOT NULL,
    updated_at          DATETIME     NOT NULL,
    INDEX idx_ventas_updated_at (updated_at)
);

SET SESSION cte_max_recursion_depth = 5000;

INSERT INTO ventas_entradas
    (fecha_partido, codigo_competicion, tribuna, canal, cantidad, precio_unitario, estado, created_at, updated_at)
WITH RECURSIVE partidos_local (fecha_partido) AS (
    SELECT DATE('2026-08-01')
    UNION ALL
    SELECT DATE_ADD(fecha_partido, INTERVAL 14 DAY)
    FROM partidos_local
    WHERE fecha_partido < '2028-12-31'
),
tribunas AS (
    SELECT 'OCCIDENTE' AS tribuna, 80.00 AS precio, 0.8 AS factor
    UNION ALL SELECT 'ORIENTE', 50.00, 1.2
    UNION ALL SELECT 'NORTE',   25.00, 0.9
    UNION ALL SELECT 'SUR',     25.00, 1.0
),
canales AS (
    SELECT 'WEB' AS canal UNION ALL SELECT 'APP' UNION ALL SELECT 'BOLETERIA'
),
dias AS (
    SELECT 0 AS dias_antes UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3
    UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6
),
base AS (
    SELECT
        p.fecha_partido,
        t.tribuna,
        t.precio,
        t.factor,
        c.canal,
        d.dias_antes,
        CRC32(CONCAT(p.fecha_partido, t.tribuna, c.canal, d.dias_antes)) AS semilla
    FROM partidos_local p
    CROSS JOIN tribunas t
    CROSS JOIN canales c
    CROSS JOIN dias d
    -- La boletería física solo abre los 2 días previos y el día del partido.
    WHERE NOT (c.canal = 'BOLETERIA' AND d.dias_antes > 2)
)
SELECT
    fecha_partido,
    'LAN',
    tribuna,
    canal,
    GREATEST(5, ROUND((40 + MOD(semilla, 260)) * factor * (1 + (6 - dias_antes) / 6))),
    precio,
    IF(MOD(semilla, 37) = 0, 'ANULADA', 'PAGADA'),
    TIMESTAMP(DATE_SUB(fecha_partido, INTERVAL dias_antes DAY)) + INTERVAL (8 + MOD(semilla, 12)) HOUR,
    TIMESTAMP(DATE_SUB(fecha_partido, INTERVAL dias_antes DAY)) + INTERVAL (8 + MOD(semilla, 12)) HOUR
        + INTERVAL IF(MOD(semilla, 37) = 0, 1, 0) DAY
FROM base
ORDER BY 8;
