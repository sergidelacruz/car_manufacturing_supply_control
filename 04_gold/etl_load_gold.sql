-- =============================================================================
-- Car Manufacturing — Gold Layer: ETL Load (Silver → Gold)
-- Layer: 04_gold
-- Description: Populates dimension tables then the fact table.
--              Order matters: dimensions first, fact second.
--              Uses MERGE for idempotency — safe to re-run.
-- =============================================================================

USE SCHEMA CAR_MANUFACTURING_DB.GOLD;

-- =============================================================================
-- STEP 1 — LOAD DIM_CUSTOMER
-- =============================================================================
MERGE INTO DIM_CUSTOMER tgt
USING (
    SELECT
        customer_id,
        customer_name,
        address,
        city,
        province,
        -- Derive a simple region grouping from province
        CASE
            WHEN province IN ('Barcelona','Girona','Lleida','Tarragona')       THEN 'Catalonia'
            WHEN province IN ('Madrid')                                         THEN 'Madrid'
            WHEN province IN ('Valencia','Alicante','Castellon')               THEN 'Valencia'
            WHEN province IN ('Seville','Malaga','Granada','Cordoba','Cadiz')  THEN 'Andalusia'
            WHEN province IN ('Vizcaya','Guipuzcoa','Alava')                   THEN 'Basque Country'
            ELSE 'Other'
        END AS region
    FROM CAR_MANUFACTURING_DB.SILVER.STG_CUSTOMERS
) src
ON tgt.customer_id = src.customer_id
WHEN MATCHED THEN UPDATE SET
    customer_name = src.customer_name,
    address       = src.address,
    city          = src.city,
    province      = src.province,
    region        = src.region,
    load_ts       = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (customer_id, customer_name, address, city, province, region)
    VALUES (src.customer_id, src.customer_name, src.address, src.city, src.province, src.region);

-- =============================================================================
-- STEP 2 — LOAD DIM_SUPPLIER
-- =============================================================================
MERGE INTO DIM_SUPPLIER tgt
USING (
    SELECT supplier_id, supplier_name, address, city, province
    FROM CAR_MANUFACTURING_DB.SILVER.STG_SUPPLIER
) src
ON tgt.supplier_id = src.supplier_id
WHEN MATCHED THEN UPDATE SET
    supplier_name = src.supplier_name,
    address       = src.address,
    city          = src.city,
    province      = src.province,
    load_ts       = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (supplier_id, supplier_name, address, city, province)
    VALUES (src.supplier_id, src.supplier_name, src.address, src.city, src.province);

-- =============================================================================
-- STEP 3 — LOAD DIM_PART
-- =============================================================================
MERGE INTO DIM_PART tgt
USING (
    SELECT part_id, part_name, color, unit_price, category_id, category_name
    FROM CAR_MANUFACTURING_DB.SILVER.STG_PARTS
) src
ON tgt.part_id = src.part_id
WHEN MATCHED THEN UPDATE SET
    part_name     = src.part_name,
    color         = src.color,
    unit_price    = src.unit_price,
    category_id   = src.category_id,
    category_name = src.category_name,
    load_ts       = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (part_id, part_name, color, unit_price, category_id, category_name)
    VALUES (src.part_id, src.part_name, src.color, src.unit_price, src.category_id, src.category_name);

-- =============================================================================
-- STEP 4 — LOAD DIM_CAR
-- Enriched with BOM aggregates and a derived segment label.
-- =============================================================================
MERGE INTO DIM_CAR tgt
USING (
    SELECT
        c.car_id,
        c.brand,
        c.model,
        c.manufacture_year,
        c.brand || ' ' || c.model                                   AS brand_model,
        -- Simple segment classification
        CASE
            WHEN c.model IN ('Born')                                 THEN 'Electric'
            WHEN c.model IN ('Q3','Arona','Tiguan','Formentor')      THEN 'SUV'
            WHEN c.model IN ('Ibiza','Polo')                         THEN 'Subcompact'
            WHEN c.model IN ('Leon','Golf','A3')                     THEN 'Compact'
            ELSE 'Other'
        END                                                         AS segment,
        COUNT(DISTINCT pc.part_id)                                  AS unique_parts,
        SUM(pc.quantity)                                            AS total_parts_qty
    FROM CAR_MANUFACTURING_DB.SILVER.STG_CARS c
    JOIN CAR_MANUFACTURING_DB.SILVER.STG_PARTS_CAR pc ON c.car_id = pc.car_id
    GROUP BY c.car_id, c.brand, c.model, c.manufacture_year
) src
ON tgt.car_id = src.car_id
WHEN MATCHED THEN UPDATE SET
    brand            = src.brand,
    model            = src.model,
    manufacture_year = src.manufacture_year,
    brand_model      = src.brand_model,
    segment          = src.segment,
    unique_parts     = src.unique_parts,
    total_parts_qty  = src.total_parts_qty,
    load_ts          = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (car_id, brand, model, manufacture_year, brand_model, segment, unique_parts, total_parts_qty)
    VALUES (src.car_id, src.brand, src.model, src.manufacture_year, src.brand_model, src.segment, src.unique_parts, src.total_parts_qty);

-- =============================================================================
-- STEP 5 — LOAD FACT_SALES
-- Resolves all natural keys to surrogate keys via dimension table joins.
-- Uses ROW_NUMBER() to handle the supplier lookup (latest delivery before sale).
-- =============================================================================
MERGE INTO FACT_SALES tgt
USING (
    WITH supplier_per_sale AS (
        -- Find the most recent supplier who delivered before or on the sale date
        SELECT
            s.sale_id,
            fd.supplier_id,
            ROW_NUMBER() OVER (
                PARTITION BY s.sale_id
                ORDER BY fd.delivery_date DESC
            ) AS rn
        FROM CAR_MANUFACTURING_DB.SILVER.STG_SALES       s
        JOIN CAR_MANUFACTURING_DB.SILVER.STG_FINAL_DELIVERY fd
            ON fd.delivery_date <= s.sale_date
    ),
    bom_counts AS (
        SELECT
            car_id,
            COUNT(DISTINCT part_id)  AS unique_parts_count,
            SUM(quantity)            AS total_parts_qty
        FROM CAR_MANUFACTURING_DB.SILVER.STG_PARTS_CAR
        GROUP BY car_id
    )
    SELECT
        dc.car_key,
        dcu.customer_key,
        ds.supplier_key,
        TO_NUMBER(TO_CHAR(ss.sale_date, 'YYYYMMDD'))        AS time_id,
        ss.sale_id,
        ss.sale_price,
        ss.total_parts_cost,
        ss.gross_margin,
        ROUND(ss.gross_margin / NULLIF(ss.sale_price, 0), 4) AS gross_margin_pct,
        bc.unique_parts_count,
        bc.total_parts_qty
    FROM CAR_MANUFACTURING_DB.SILVER.STG_SALES           ss
    JOIN DIM_CAR           dc  ON ss.car_id      = dc.car_id
    JOIN DIM_CUSTOMER      dcu ON ss.customer_id = dcu.customer_id
    JOIN supplier_per_sale sps ON ss.sale_id     = sps.sale_id AND sps.rn = 1
    JOIN DIM_SUPPLIER      ds  ON sps.supplier_id = ds.supplier_id
    JOIN bom_counts        bc  ON ss.car_id       = bc.car_id
) src
ON tgt.sale_id = src.sale_id
WHEN MATCHED THEN UPDATE SET
    car_key            = src.car_key,
    customer_key       = src.customer_key,
    supplier_key       = src.supplier_key,
    time_id            = src.time_id,
    sale_price         = src.sale_price,
    total_parts_cost   = src.total_parts_cost,
    gross_margin       = src.gross_margin,
    gross_margin_pct   = src.gross_margin_pct,
    unique_parts_count = src.unique_parts_count,
    total_parts_qty    = src.total_parts_qty,
    load_ts            = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
    car_key, customer_key, supplier_key, time_id, sale_id,
    sale_price, total_parts_cost, gross_margin, gross_margin_pct,
    unique_parts_count, total_parts_qty
)
VALUES (
    src.car_key, src.customer_key, src.supplier_key, src.time_id, src.sale_id,
    src.sale_price, src.total_parts_cost, src.gross_margin, src.gross_margin_pct,
    src.unique_parts_count, src.total_parts_qty
);
