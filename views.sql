-- =============================================================================
-- Car Manufacturing — Reporting Layer: Analytical Views
-- Layer: 05_reporting
-- Description: Business-facing views built on top of the Gold star schema.
--              These are the queries a BI tool (Tableau, Power BI, Streamlit)
--              or an analyst would run directly.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS CAR_MANUFACTURING_DB.REPORTING;
USE SCHEMA CAR_MANUFACTURING_DB.REPORTING;

-- =============================================================================
-- VIEW 1 — Revenue by brand and year
-- Shows total revenue, total parts cost, and gross margin per brand per year.
-- =============================================================================
CREATE OR REPLACE VIEW RPT_REVENUE_BY_BRAND AS
SELECT
    t.year,
    t.quarter_name,
    c.brand,
    COUNT(DISTINCT f.sale_key)              AS total_sales,
    SUM(f.sale_price)                       AS total_revenue,
    SUM(f.total_parts_cost)                 AS total_parts_cost,
    SUM(f.gross_margin)                     AS total_gross_margin,
    ROUND(AVG(f.gross_margin_pct) * 100, 2) AS avg_margin_pct,
    ROUND(AVG(f.sale_price), 2)             AS avg_sale_price
FROM CAR_MANUFACTURING_DB.GOLD.FACT_SALES       f
JOIN CAR_MANUFACTURING_DB.GOLD.DIM_CAR          c ON f.car_key  = c.car_key
JOIN CAR_MANUFACTURING_DB.GOLD.DIM_TIME         t ON f.time_id  = t.time_id
GROUP BY t.year, t.quarter_name, c.brand
ORDER BY t.year, t.quarter_name, total_revenue DESC;

-- =============================================================================
-- VIEW 2 — Sales by customer region
-- Helps answer: which regions generate the most revenue?
-- =============================================================================
CREATE OR REPLACE VIEW RPT_SALES_BY_REGION AS
SELECT
    cu.region,
    cu.province,
    t.year,
    t.month_name,
    COUNT(DISTINCT f.sale_key)              AS total_sales,
    SUM(f.sale_price)                       AS total_revenue,
    SUM(f.gross_margin)                     AS total_gross_margin,
    ROUND(AVG(f.sale_price), 2)             AS avg_sale_price
FROM CAR_MANUFACTURING_DB.GOLD.FACT_SALES       f
JOIN CAR_MANUFACTURING_DB.GOLD.DIM_CUSTOMER     cu ON f.customer_key = cu.customer_key
JOIN CAR_MANUFACTURING_DB.GOLD.DIM_TIME         t  ON f.time_id      = t.time_id
GROUP BY cu.region, cu.province, t.year, t.month_name, t.month
ORDER BY t.year, t.month, total_revenue DESC;

-- =============================================================================
-- VIEW 3 — Top car models by gross margin
-- Shows which models are most profitable, with segment breakdown.
-- =============================================================================
CREATE OR REPLACE VIEW RPT_MODEL_PROFITABILITY AS
SELECT
    c.brand,
    c.model,
    c.brand_model,
    c.segment,
    c.manufacture_year,
    COUNT(DISTINCT f.sale_key)              AS units_sold,
    SUM(f.sale_price)                       AS total_revenue,
    SUM(f.total_parts_cost)                 AS total_parts_cost,
    SUM(f.gross_margin)                     AS total_gross_margin,
    ROUND(AVG(f.gross_margin_pct) * 100, 2) AS avg_margin_pct,
    ROUND(SUM(f.gross_margin)
          / NULLIF(COUNT(DISTINCT f.sale_key), 0), 2) AS margin_per_unit
FROM CAR_MANUFACTURING_DB.GOLD.FACT_SALES   f
JOIN CAR_MANUFACTURING_DB.GOLD.DIM_CAR      c ON f.car_key = c.car_key
GROUP BY c.brand, c.model, c.brand_model, c.segment, c.manufacture_year
ORDER BY avg_margin_pct DESC;

-- =============================================================================
-- VIEW 4 — Supplier delivery performance
-- Links deliveries to sales — how much value each supplier feeds into the chain.
-- =============================================================================
CREATE OR REPLACE VIEW RPT_SUPPLIER_PERFORMANCE AS
SELECT
    s.supplier_name,
    s.city                                  AS supplier_city,
    t.year,
    t.quarter_name,
    COUNT(DISTINCT f.sale_key)              AS sales_linked,
    SUM(f.total_parts_cost)                 AS parts_value_supplied,
    SUM(f.sale_price)                       AS revenue_enabled,
    ROUND(SUM(f.total_parts_cost)
          / NULLIF(SUM(f.sale_price), 0) * 100, 2) AS cost_to_revenue_pct
FROM CAR_MANUFACTURING_DB.GOLD.FACT_SALES   f
JOIN CAR_MANUFACTURING_DB.GOLD.DIM_SUPPLIER s ON f.supplier_key = s.supplier_key
JOIN CAR_MANUFACTURING_DB.GOLD.DIM_TIME     t ON f.time_id      = t.time_id
GROUP BY s.supplier_name, s.city, t.year, t.quarter_name
ORDER BY t.year, t.quarter_name, parts_value_supplied DESC;

-- =============================================================================
-- VIEW 5 — Monthly sales trend (time series)
-- Ready for a line chart in any BI tool.
-- =============================================================================
CREATE OR REPLACE VIEW RPT_MONTHLY_TREND AS
SELECT
    t.year,
    t.month,
    t.month_short,
    t.year || '-' || LPAD(t.month::VARCHAR, 2, '0') AS year_month,
    COUNT(DISTINCT f.sale_key)              AS total_sales,
    SUM(f.sale_price)                       AS total_revenue,
    SUM(f.gross_margin)                     AS total_gross_margin,
    ROUND(AVG(f.gross_margin_pct) * 100, 2) AS avg_margin_pct,
    -- Month-over-month revenue change
    LAG(SUM(f.sale_price)) OVER (ORDER BY t.year, t.month) AS prev_month_revenue,
    ROUND(
        (SUM(f.sale_price) - LAG(SUM(f.sale_price)) OVER (ORDER BY t.year, t.month))
        / NULLIF(LAG(SUM(f.sale_price)) OVER (ORDER BY t.year, t.month), 0) * 100
    , 2)                                    AS mom_revenue_growth_pct
FROM CAR_MANUFACTURING_DB.GOLD.FACT_SALES   f
JOIN CAR_MANUFACTURING_DB.GOLD.DIM_TIME     t ON f.time_id = t.time_id
GROUP BY t.year, t.month, t.month_short
ORDER BY t.year, t.month;

-- =============================================================================
-- VIEW 6 — Parts cost breakdown by category per car segment
-- Answers: where does the BOM cost go for each segment?
-- =============================================================================
CREATE OR REPLACE VIEW RPT_PARTS_COST_BY_SEGMENT AS
SELECT
    dc.segment,
    dp.category_name,
    COUNT(DISTINCT f.sale_key)              AS units_sold,
    SUM(dp.unit_price * dc.total_parts_qty) AS estimated_category_cost,
    ROUND(
        SUM(dp.unit_price * dc.total_parts_qty)
        / NULLIF(SUM(SUM(dp.unit_price * dc.total_parts_qty))
            OVER (PARTITION BY dc.segment), 0) * 100
    , 2)                                    AS pct_of_segment_cost
FROM CAR_MANUFACTURING_DB.GOLD.FACT_SALES   f
JOIN CAR_MANUFACTURING_DB.GOLD.DIM_CAR      dc ON f.car_key  = dc.car_key
JOIN CAR_MANUFACTURING_DB.GOLD.DIM_PART     dp ON dp.part_key IN (
    -- All parts that belong to cars in this fact row's car segment
    SELECT DISTINCT dpart.part_key
    FROM CAR_MANUFACTURING_DB.GOLD.DIM_PART dpart
    JOIN CAR_MANUFACTURING_DB.SOURCE.PARTS_CAR pc ON dpart.part_id = pc.Pieza_ID
    JOIN CAR_MANUFACTURING_DB.GOLD.DIM_CAR dcar   ON pc.Car_ID     = dcar.car_id
    WHERE dcar.segment = dc.segment
)
GROUP BY dc.segment, dp.category_name
ORDER BY dc.segment, estimated_category_cost DESC;
