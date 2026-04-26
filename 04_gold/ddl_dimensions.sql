-- =============================================================================
-- Car Manufacturing — Gold Layer: Dimension Table DDL
-- Layer: 04_gold
-- Description: Creates the five dimension tables of the Star Schema.
--              Uses surrogate keys (auto-increment) separate from source IDs,
--              following Kimball dimensional modelling best practices.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS CAR_MANUFACTURING_DB.GOLD;
USE SCHEMA CAR_MANUFACTURING_DB.GOLD;

-- -----------------------------------------------------------------------------
-- DIM_TIME
-- Date dimension pre-populated for the full range needed.
-- Decoupled from any source table — generated procedurally.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE DIM_TIME (
    time_id         INTEGER         NOT NULL,   -- YYYYMMDD integer key
    full_date       DATE            NOT NULL,
    year            INTEGER         NOT NULL,
    quarter         INTEGER         NOT NULL,   -- 1–4
    quarter_name    VARCHAR(6)      NOT NULL,   -- 'Q1'…'Q4'
    month           INTEGER         NOT NULL,   -- 1–12
    month_name      VARCHAR(20)     NOT NULL,   -- 'January'…
    month_short     VARCHAR(3)      NOT NULL,   -- 'Jan'…
    week_of_year    INTEGER         NOT NULL,
    day_of_month    INTEGER         NOT NULL,
    day_of_week     INTEGER         NOT NULL,   -- 0=Sun … 6=Sat
    day_name        VARCHAR(10)     NOT NULL,
    is_weekend      BOOLEAN         NOT NULL,

    CONSTRAINT PK_DIM_TIME PRIMARY KEY (time_id)
);

-- Populate DIM_TIME for 2022-01-01 → 2025-12-31
INSERT INTO DIM_TIME
WITH date_spine AS (
    SELECT DATEADD(DAY, SEQ4(), '2022-01-01'::DATE) AS d
    FROM TABLE(GENERATOR(ROWCOUNT => 1461))   -- 4 years
)
SELECT
    TO_NUMBER(TO_CHAR(d, 'YYYYMMDD'))           AS time_id,
    d                                           AS full_date,
    YEAR(d)                                     AS year,
    QUARTER(d)                                  AS quarter,
    'Q' || QUARTER(d)::VARCHAR                  AS quarter_name,
    MONTH(d)                                    AS month,
    MONTHNAME(d)                                AS month_name,
    LEFT(MONTHNAME(d), 3)                       AS month_short,
    WEEKOFYEAR(d)                               AS week_of_year,
    DAY(d)                                      AS day_of_month,
    DAYOFWEEK(d)                                AS day_of_week,
    DAYNAME(d)                                  AS day_name,
    CASE WHEN DAYOFWEEK(d) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend
FROM date_spine;

-- -----------------------------------------------------------------------------
-- DIM_CUSTOMER
-- SCD Type 1 — overwrite on change (no history kept, suitable for this scope).
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE DIM_CUSTOMER (
    customer_key    INTEGER         NOT NULL AUTOINCREMENT START 1 INCREMENT 1,
    customer_id     INTEGER         NOT NULL,   -- source natural key
    customer_name   NVARCHAR(100)   NOT NULL,
    address         NVARCHAR(200),
    city            NVARCHAR(100),
    province        NVARCHAR(100),
    region          VARCHAR(50),                -- derived grouping (e.g. 'Northeast')
    load_ts         TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),

    CONSTRAINT PK_DIM_CUSTOMER PRIMARY KEY (customer_key)
);

-- -----------------------------------------------------------------------------
-- DIM_CAR
-- Enriched with total parts count (pre-aggregated for reporting performance).
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE DIM_CAR (
    car_key         INTEGER         NOT NULL AUTOINCREMENT START 1 INCREMENT 1,
    car_id          INTEGER         NOT NULL,   -- source natural key
    brand           TEXT            NOT NULL,
    model           TEXT            NOT NULL,
    manufacture_year INTEGER        NOT NULL,
    brand_model     TEXT            NOT NULL,   -- derived: 'SEAT Ibiza'
    segment         VARCHAR(30),                -- derived: 'Compact', 'SUV', 'Electric'
    unique_parts    INTEGER,                    -- count of distinct parts
    total_parts_qty INTEGER,                    -- total BOM quantity
    load_ts         TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),

    CONSTRAINT PK_DIM_CAR PRIMARY KEY (car_key)
);

-- -----------------------------------------------------------------------------
-- DIM_SUPPLIER
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE DIM_SUPPLIER (
    supplier_key    INTEGER         NOT NULL AUTOINCREMENT START 1 INCREMENT 1,
    supplier_id     INTEGER         NOT NULL,   -- source natural key
    supplier_name   NVARCHAR(100)   NOT NULL,
    address         NVARCHAR(200),
    city            NVARCHAR(100),
    province        NVARCHAR(100),
    load_ts         TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),

    CONSTRAINT PK_DIM_SUPPLIER PRIMARY KEY (supplier_key)
);

-- -----------------------------------------------------------------------------
-- DIM_PART
-- Denormalised: category name folded in (avoids a snowflake join at query time).
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE DIM_PART (
    part_key        INTEGER         NOT NULL AUTOINCREMENT START 1 INCREMENT 1,
    part_id         INTEGER         NOT NULL,   -- source natural key
    part_name       TEXT            NOT NULL,
    color           TEXT,
    unit_price      DECIMAL(10,2)   NOT NULL,
    category_id     INTEGER         NOT NULL,
    category_name   TEXT            NOT NULL,
    load_ts         TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),

    CONSTRAINT PK_DIM_PART PRIMARY KEY (part_key)
);
