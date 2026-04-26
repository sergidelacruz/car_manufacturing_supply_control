-- =============================================================================
-- Car Manufacturing — Silver Layer: Data Quality & Cleansing
-- Layer: 03_silver
-- Description: Validates source data, flags issues, and produces clean staging
--              tables ready for dimensional modelling.
--              Run this BEFORE loading the Gold layer.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS CAR_MANUFACTURING_DB.SILVER;
USE SCHEMA CAR_MANUFACTURING_DB.SILVER;

-- =============================================================================
-- SECTION 1 — DATA QUALITY CHECKS
-- These are assertion-style queries. Each should return 0 rows.
-- In Snowflake you can wrap these in a task or stored procedure and alert
-- if COUNT > 0.
-- =============================================================================

-- ----------------------------------------------------------------------------
-- QC-01: NULL primary keys
-- ----------------------------------------------------------------------------
SELECT 'QC-01' AS check_id, 'NULL PK in SUPPLIER' AS description, COUNT(*) AS failures
FROM CAR_MANUFACTURING_DB.SOURCE.SUPPLIER WHERE Proveedor_ID IS NULL
UNION ALL
SELECT 'QC-01', 'NULL PK in CUSTOMERS',  COUNT(*) FROM CAR_MANUFACTURING_DB.SOURCE.CUSTOMERS  WHERE Cliente_ID IS NULL
UNION ALL
SELECT 'QC-01', 'NULL PK in CARS',       COUNT(*) FROM CAR_MANUFACTURING_DB.SOURCE.CARS       WHERE Car_ID IS NULL
UNION ALL
SELECT 'QC-01', 'NULL PK in PARTS',      COUNT(*) FROM CAR_MANUFACTURING_DB.SOURCE.PARTS      WHERE Pieza_ID IS NULL
UNION ALL
SELECT 'QC-01', 'NULL PK in SALES',      COUNT(*) FROM CAR_MANUFACTURING_DB.SOURCE.SALES      WHERE Venta_ID IS NULL;

-- ----------------------------------------------------------------------------
-- QC-02: Negative or zero prices
-- ----------------------------------------------------------------------------
SELECT 'QC-02' AS check_id, 'Zero/negative price in PARTS' AS description, COUNT(*) AS failures
FROM CAR_MANUFACTURING_DB.SOURCE.PARTS WHERE Precio <= 0
UNION ALL
SELECT 'QC-02', 'Zero/negative price in SALES', COUNT(*)
FROM CAR_MANUFACTURING_DB.SOURCE.SALES WHERE Precio <= 0;

-- ----------------------------------------------------------------------------
-- QC-03: Orphan foreign keys
-- ----------------------------------------------------------------------------
SELECT 'QC-03' AS check_id, 'SALES referencing missing Car_ID' AS description, COUNT(*) AS failures
FROM CAR_MANUFACTURING_DB.SOURCE.SALES s
LEFT JOIN CAR_MANUFACTURING_DB.SOURCE.CARS c ON s.Car_ID = c.Car_ID
WHERE c.Car_ID IS NULL
UNION ALL
SELECT 'QC-03', 'SALES referencing missing Cliente_ID', COUNT(*)
FROM CAR_MANUFACTURING_DB.SOURCE.SALES s
LEFT JOIN CAR_MANUFACTURING_DB.SOURCE.CUSTOMERS cu ON s.Cliente_ID = cu.Cliente_ID
WHERE cu.Cliente_ID IS NULL
UNION ALL
SELECT 'QC-03', 'PARTS referencing missing Category_ID', COUNT(*)
FROM CAR_MANUFACTURING_DB.SOURCE.PARTS p
LEFT JOIN CAR_MANUFACTURING_DB.SOURCE.CATEGORY ca ON p.Category_ID = ca.Category_ID
WHERE ca.Category_ID IS NULL;

-- ----------------------------------------------------------------------------
-- QC-04: Future dates
-- ----------------------------------------------------------------------------
SELECT 'QC-04' AS check_id, 'Future dates in SALES' AS description, COUNT(*) AS failures
FROM CAR_MANUFACTURING_DB.SOURCE.SALES WHERE Fecha > CURRENT_DATE()
UNION ALL
SELECT 'QC-04', 'Future dates in FINAL_DELIVERY', COUNT(*)
FROM CAR_MANUFACTURING_DB.SOURCE.FINAL_DELIVERY WHERE Fecha > CURRENT_DATE();

-- ----------------------------------------------------------------------------
-- QC-05: Duplicate primary keys
-- ----------------------------------------------------------------------------
SELECT 'QC-05' AS check_id, 'Duplicate Venta_ID in SALES' AS description, COUNT(*) - COUNT(DISTINCT Venta_ID) AS failures
FROM CAR_MANUFACTURING_DB.SOURCE.SALES
UNION ALL
SELECT 'QC-05', 'Duplicate Car_ID in CARS', COUNT(*) - COUNT(DISTINCT Car_ID)
FROM CAR_MANUFACTURING_DB.SOURCE.CARS;

-- ----------------------------------------------------------------------------
-- QC-06: Business rule — sale price must be higher than total parts cost
-- ----------------------------------------------------------------------------
SELECT 'QC-06' AS check_id, 'Sale price below parts cost (potential data error)' AS description, COUNT(*) AS failures
FROM (
    SELECT
        s.Venta_ID,
        s.Precio AS sale_price,
        SUM(p.Precio * pc.Cantidad_pieza) AS total_parts_cost
    FROM CAR_MANUFACTURING_DB.SOURCE.SALES s
    JOIN CAR_MANUFACTURING_DB.SOURCE.PARTS_CAR  pc ON s.Car_ID    = pc.Car_ID
    JOIN CAR_MANUFACTURING_DB.SOURCE.PARTS       p  ON pc.Pieza_ID = p.Pieza_ID
    GROUP BY s.Venta_ID, s.Precio
) sub
WHERE sale_price < total_parts_cost;

-- =============================================================================
-- SECTION 2 — CLEAN STAGING TABLES
-- These views standardise naming to English, cast types correctly, and add
-- derived columns (load_ts, is_valid flags). Views keep this zero-copy.
-- =============================================================================

CREATE OR REPLACE VIEW SILVER.STG_SUPPLIER AS
SELECT
    Proveedor_ID                        AS supplier_id,
    TRIM(Nombre)                        AS supplier_name,
    TRIM(COALESCE(Direccion, 'Unknown')) AS address,
    TRIM(COALESCE(Ciudad,    'Unknown')) AS city,
    TRIM(COALESCE(Provincia, 'Unknown')) AS province,
    CURRENT_TIMESTAMP()                 AS load_ts
FROM CAR_MANUFACTURING_DB.SOURCE.SUPPLIER;

CREATE OR REPLACE VIEW SILVER.STG_CUSTOMERS AS
SELECT
    Cliente_ID                          AS customer_id,
    TRIM(Nombre)                        AS customer_name,
    TRIM(COALESCE(Direccion, 'Unknown')) AS address,
    TRIM(COALESCE(Ciudad,    'Unknown')) AS city,
    TRIM(COALESCE(Provincia, 'Unknown')) AS province,
    CURRENT_TIMESTAMP()                 AS load_ts
FROM CAR_MANUFACTURING_DB.SOURCE.CUSTOMERS;

CREATE OR REPLACE VIEW SILVER.STG_CARS AS
SELECT
    Car_ID                              AS car_id,
    TRIM(Marca)                         AS brand,
    TRIM(Modelo)                        AS model,
    Ano::INTEGER                        AS manufacture_year,
    CURRENT_TIMESTAMP()                 AS load_ts
FROM CAR_MANUFACTURING_DB.SOURCE.CARS;

CREATE OR REPLACE VIEW SILVER.STG_CATEGORY AS
SELECT
    Category_ID                         AS category_id,
    TRIM(Nombre)                        AS category_name,
    CURRENT_TIMESTAMP()                 AS load_ts
FROM CAR_MANUFACTURING_DB.SOURCE.CATEGORY;

CREATE OR REPLACE VIEW SILVER.STG_PARTS AS
SELECT
    p.Pieza_ID                          AS part_id,
    TRIM(p.Nombre)                      AS part_name,
    TRIM(COALESCE(p.Color, 'Unknown'))  AS color,
    p.Precio::DECIMAL(10,2)             AS unit_price,
    p.Category_ID                       AS category_id,
    TRIM(c.Nombre)                      AS category_name,
    CURRENT_TIMESTAMP()                 AS load_ts
FROM CAR_MANUFACTURING_DB.SOURCE.PARTS p
JOIN CAR_MANUFACTURING_DB.SOURCE.CATEGORY c ON p.Category_ID = c.Category_ID;

CREATE OR REPLACE VIEW SILVER.STG_SALES AS
SELECT
    s.Venta_ID                          AS sale_id,
    s.Cliente_ID                        AS customer_id,
    s.Car_ID                            AS car_id,
    s.Fecha::DATE                       AS sale_date,
    s.Precio::NUMERIC(12,2)             AS sale_price,
    -- Derived: total parts cost for this car
    SUM(p.Precio * pc.Cantidad_pieza)   AS total_parts_cost,
    -- Derived: gross margin
    s.Precio - SUM(p.Precio * pc.Cantidad_pieza) AS gross_margin,
    CURRENT_TIMESTAMP()                 AS load_ts
FROM CAR_MANUFACTURING_DB.SOURCE.SALES s
JOIN CAR_MANUFACTURING_DB.SOURCE.PARTS_CAR pc ON s.Car_ID    = pc.Car_ID
JOIN CAR_MANUFACTURING_DB.SOURCE.PARTS      p  ON pc.Pieza_ID = p.Pieza_ID
GROUP BY s.Venta_ID, s.Cliente_ID, s.Car_ID, s.Fecha, s.Precio;

CREATE OR REPLACE VIEW SILVER.STG_PARTS_CAR AS
SELECT
    Car_ID                              AS car_id,
    Pieza_ID                            AS part_id,
    Cantidad_pieza                      AS quantity,
    CURRENT_TIMESTAMP()                 AS load_ts
FROM CAR_MANUFACTURING_DB.SOURCE.PARTS_CAR;

CREATE OR REPLACE VIEW SILVER.STG_FINAL_DELIVERY AS
SELECT
    fd.Entrega_ID                       AS delivery_id,
    fd.Proveedor_ID                     AS supplier_id,
    fd.Fecha::DATE                      AS delivery_date,
    CURRENT_TIMESTAMP()                 AS load_ts
FROM CAR_MANUFACTURING_DB.SOURCE.FINAL_DELIVERY fd;

CREATE OR REPLACE VIEW SILVER.STG_LOT_DELIVERY AS
SELECT
    ld.Entrega_ID                       AS delivery_id,
    ld.Pieza_ID                         AS part_id,
    ld.Fecha::DATE                      AS delivery_date,
    ld.Cantidad                         AS quantity_delivered,
    CURRENT_TIMESTAMP()                 AS load_ts
FROM CAR_MANUFACTURING_DB.SOURCE.LOT_DELIVERY ld;
