-- =============================================================================
-- Car Manufacturing — Gold Layer: Fact Table DDL
-- Layer: 04_gold
-- Description: Central fact table for sales transactions.
--              Grain: one row per car sale.
--              All foreign keys reference surrogate keys in dimension tables.
-- =============================================================================

USE SCHEMA CAR_MANUFACTURING_DB.GOLD;

-- -----------------------------------------------------------------------------
-- FACT_SALES
-- Grain: one record per vehicle sale (Venta_ID from source).
-- Measures: sale_price, parts_cost, gross_margin, parts_used.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE FACT_SALES (
    -- Surrogate key
    sale_key            INTEGER         NOT NULL AUTOINCREMENT START 1 INCREMENT 1,

    -- Foreign keys to dimensions
    car_key             INTEGER         NOT NULL,
    customer_key        INTEGER         NOT NULL,
    supplier_key        INTEGER         NOT NULL,   -- primary supplier for delivery
    time_id             INTEGER         NOT NULL,   -- FK to DIM_TIME (YYYYMMDD)

    -- Degenerate dimension (source transaction ID — no dim table needed)
    sale_id             INTEGER         NOT NULL,

    -- Additive measures
    sale_price          NUMERIC(12,2)   NOT NULL,
    total_parts_cost    DECIMAL(12,2)   NOT NULL,
    gross_margin        DECIMAL(12,2)   NOT NULL,   -- sale_price - total_parts_cost
    gross_margin_pct    DECIMAL(6,4),               -- gross_margin / sale_price
    unique_parts_count  INTEGER         NOT NULL,   -- distinct parts in BOM
    total_parts_qty     INTEGER         NOT NULL,   -- total quantity across BOM

    -- Audit
    load_ts             TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),

    -- Constraints
    CONSTRAINT PK_FACT_SALES    PRIMARY KEY (sale_key),
    CONSTRAINT FK_FS_CAR        FOREIGN KEY (car_key)       REFERENCES DIM_CAR(car_key),
    CONSTRAINT FK_FS_CUSTOMER   FOREIGN KEY (customer_key)  REFERENCES DIM_CUSTOMER(customer_key),
    CONSTRAINT FK_FS_SUPPLIER   FOREIGN KEY (supplier_key)  REFERENCES DIM_SUPPLIER(supplier_key),
    CONSTRAINT FK_FS_TIME       FOREIGN KEY (time_id)       REFERENCES DIM_TIME(time_id)
);
