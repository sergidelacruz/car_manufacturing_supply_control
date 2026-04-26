# Car Manufacturing Supply Control — Data Engineering Project

A end-to-end Data Engineering portfolio project built on Snowflake, demonstrating data modelling, data structuring, and data management skills using a car manufacturing supply chain database.

---

## Project overview

This project transforms a transactional (OLTP) database for a car manufacturer into a fully modelled analytical data warehouse following the **Medallion Architecture** (Bronze → Silver → Gold) and **Kimball's Star Schema** dimensional modelling principles.

### Skills demonstrated

| Skill | Where |
|-------|-------|
| Data modelling — Star Schema design | `04_gold/ddl_dimensions.sql`, `04_gold/ddl_fact.sql` |
| Data structuring — DDL, types, constraints | `01_source_schema/ddl_source.sql` |
| ETL / ELT — MERGE, window functions, derived columns | `04_gold/etl_load_gold.sql` |
| Data quality — assertion checks, NULL handling | `03_silver/quality_checks_and_staging.sql` |
| Data management — dictionary, lineage, business rules | `docs/data_dictionary.md` |
| Analytical SQL — views, LAG, PARTITION BY | `05_reporting/views.sql` |
| Snowflake-native features — GENERATOR, AUTOINCREMENT, MERGE | Throughout |

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│  SOURCE schema (OLTP)                                           │
│  SUPPLIER · FINAL_DELIVERY · LOT_DELIVERY · PARTS · CATEGORY   │
│  CARS · PARTS_CAR · CUSTOMERS · SALES                          │
└───────────────────────┬─────────────────────────────────────────┘
                        │ Silver views (rename, cast, QC checks)
┌───────────────────────▼─────────────────────────────────────────┐
│  SILVER schema (Staging)                                        │
│  STG_SUPPLIER · STG_CUSTOMERS · STG_CARS · STG_PARTS           │
│  STG_SALES · STG_PARTS_CAR · STG_FINAL_DELIVERY                │
└───────────────────────┬─────────────────────────────────────────┘
                        │ MERGE (idempotent ETL)
┌───────────────────────▼─────────────────────────────────────────┐
│  GOLD schema (Star Schema)                                      │
│  DIM_TIME · DIM_CAR · DIM_CUSTOMER · DIM_SUPPLIER · DIM_PART   │
│                      FACT_SALES                                 │
└───────────────────────┬─────────────────────────────────────────┘
                        │ Analytical views
┌───────────────────────▼─────────────────────────────────────────┐
│  REPORTING schema                                               │
│  RPT_REVENUE_BY_BRAND · RPT_SALES_BY_REGION                    │
│  RPT_MODEL_PROFITABILITY · RPT_SUPPLIER_PERFORMANCE            │
│  RPT_MONTHLY_TREND · RPT_PARTS_COST_BY_SEGMENT                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## Project structure

```
car-manufacturing-de-project/
├── README.md
├── 01_source_schema/
│   └── ddl_source.sql              Original 8 OLTP tables
├── 02_bronze/
│   └── seed_data.sql               Sample data (30 sales, 20 parts, 10 cars…)
├── 03_silver/
│   └── quality_checks_and_staging.sql   6 QC checks + 8 staging views
├── 04_gold/
│   ├── ddl_dimensions.sql          DIM_TIME, DIM_CAR, DIM_CUSTOMER, DIM_SUPPLIER, DIM_PART
│   ├── ddl_fact.sql                FACT_SALES
│   └── etl_load_gold.sql           MERGE-based load: Silver → Gold
├── 05_reporting/
│   └── views.sql                   6 analytical views
└── docs/
    └── data_dictionary.md          Field definitions, business rules, lineage
```

---

## How to run

Execute the scripts in order inside a Snowflake worksheet:

```sql
-- 1. Create source schema and tables
-- File: 01_source_schema/ddl_source.sql

-- 2. Load sample data
-- File: 02_bronze/seed_data.sql

-- 3. Run data quality checks (all should return 0 failures)
-- File: 03_silver/quality_checks_and_staging.sql

-- 4. Create dimension tables and populate DIM_TIME
-- File: 04_gold/ddl_dimensions.sql

-- 5. Create fact table
-- File: 04_gold/ddl_fact.sql

-- 6. Run ETL: Silver → Gold (MERGEs)
-- File: 04_gold/etl_load_gold.sql

-- 7. Create reporting views
-- File: 05_reporting/views.sql

-- 8. Query away:
SELECT * FROM CAR_MANUFACTURING_DB.REPORTING.RPT_REVENUE_BY_BRAND;
SELECT * FROM CAR_MANUFACTURING_DB.REPORTING.RPT_MODEL_PROFITABILITY ORDER BY avg_margin_pct DESC;
SELECT * FROM CAR_MANUFACTURING_DB.REPORTING.RPT_MONTHLY_TREND;
```

---

## Key design decisions

**Surrogate keys over natural keys** — All dimension tables use `AUTOINCREMENT` surrogate keys. This decouples the warehouse from source system ID changes and is required for SCD patterns.

**MERGE for idempotency** — All Gold loads use `MERGE INTO ... USING ... ON ...` so scripts can be re-run safely without creating duplicates. This is production-grade ETL practice.

**DIM_TIME generated, not derived** — The date dimension is populated procedurally using Snowflake's `GENERATOR` function rather than derived from transaction dates. This ensures every date exists even if no sales occurred on that day.

**Silver as views, not tables** — The Silver layer uses `CREATE OR REPLACE VIEW` rather than materialised tables. This keeps storage minimal and ensures Silver always reflects the latest source data. In a production pipeline with large volumes you would switch to materialised tables or dynamic tables.

**Gross margin pre-computed in fact** — `gross_margin` and `gross_margin_pct` are stored in the fact table rather than computed at query time. This follows the Kimball convention of storing additive measures in the fact table for maximum query performance.

**Supplier linked via latest delivery** — Since there is no direct foreign key between SALES and SUPPLIER in the source schema, the ETL uses `ROW_NUMBER() OVER (PARTITION BY sale_id ORDER BY delivery_date DESC)` to find the most recent supplier delivery on or before each sale date. This demonstrates a common real-world data linkage pattern.

---

## Sample queries

```sql
-- Top 3 most profitable car models
SELECT brand_model, avg_margin_pct, units_sold, total_gross_margin
FROM CAR_MANUFACTURING_DB.REPORTING.RPT_MODEL_PROFITABILITY
ORDER BY avg_margin_pct DESC
LIMIT 3;

-- Monthly revenue growth in 2023
SELECT year_month, total_revenue, mom_revenue_growth_pct
FROM CAR_MANUFACTURING_DB.REPORTING.RPT_MONTHLY_TREND
WHERE year = 2023
ORDER BY month;

-- Revenue by region
SELECT region, SUM(total_revenue) AS revenue
FROM CAR_MANUFACTURING_DB.REPORTING.RPT_SALES_BY_REGION
GROUP BY region
ORDER BY revenue DESC;
```

---

## Technologies

- **Snowflake** — Cloud data warehouse (all SQL is Snowflake dialect)
- **SQL** — DDL, DML, window functions, CTEs, MERGE
- **Kimball dimensional modelling** — Star schema, surrogate keys, SCD Type 1
- **Medallion architecture** — Bronze / Silver / Gold layering

---

## Author

Built as a Data Engineering portfolio project. Feel free to fork and extend — e.g. add a Snowflake Task to schedule the ETL, a Stream for CDC, or connect a Streamlit dashboard on top of the reporting views.
