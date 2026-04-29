# Automotive Analytics Platform — Analytics Engineering with Snowflake

![SQL](https://img.shields.io/badge/SQL-Snowflake-29B5E8?logo=snowflake&logoColor=white)
![Architecture](https://img.shields.io/badge/Architecture-Medallion-brightgreen)
![Modelling](https://img.shields.io/badge/Modelling-Star%20Schema%20%28Kimball%29-orange)
![dbt](https://img.shields.io/badge/dbt--compatible-architecture-FF694B?logo=dbt&logoColor=white)
![Status](https://img.shields.io/badge/Status-Complete-success)

An end-to-end **analytics engineering** project built on Snowflake. Transforms a transactional OLTP database from a car manufacturing company into a fully modelled analytical data warehouse — enabling business teams to answer questions about revenue, supplier performance, and model profitability without touching raw data.

**Business context:** A car manufacturer needs to consolidate data from sales, suppliers, parts, and deliveries into a single analytical layer. The goal is a reliable, self-service reporting layer that answers questions like: *Which models have the highest margin? Which suppliers are underperforming? How is revenue trending month-over-month?*

![Architecture Diagram](docs/architecture.svg)

---

## Analytics engineering patterns demonstrated

| Pattern | Implementation | Why it matters |
|---------|---------------|----------------|
| Dimensional modelling (Kimball) | Star schema: 5 dims + 1 fact | Query performance, BI-tool compatibility |
| Medallion architecture | Bronze → Silver → Gold → Reporting | Clear data lineage, layer isolation |
| Idempotent ETL | `MERGE INTO ... USING ... ON ...` throughout | Safe re-runs, no duplicates in production |
| Surrogate keys | `AUTOINCREMENT` on all dimension tables | Decouples warehouse from source system changes |
| Data quality layer | 6 automated assertion checks in Silver | Catches issues before they reach Gold |
| Semantic / reporting layer | 6 analytical views on top of Gold | Decouples BI tools from warehouse internals |
| Generated date dimension | `GENERATOR` function — pre-populated 2022–2025 | Every date exists, even days with no sales |

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│  SOURCE schema (OLTP)                                           │
│  SUPPLIER · FINAL_DELIVERY · LOT_DELIVERY · PARTS · CATEGORY   │
│  CARS · PARTS_CAR · CUSTOMERS · SALES                          │
└───────────────────────┬─────────────────────────────────────────┘
                        │ Silver views — rename, cast, QC checks
┌───────────────────────▼─────────────────────────────────────────┐
│  SILVER schema (Staging)                                        │
│  STG_SUPPLIER · STG_CUSTOMERS · STG_CARS · STG_PARTS           │
│  STG_SALES · STG_PARTS_CAR · STG_FINAL_DELIVERY                │
└───────────────────────┬─────────────────────────────────────────┘
                        │ MERGE — idempotent ETL load
┌───────────────────────▼─────────────────────────────────────────┐
│  GOLD schema (Star Schema)                                      │
│  DIM_TIME · DIM_CAR · DIM_CUSTOMER · DIM_SUPPLIER · DIM_PART   │
│                      FACT_SALES                                 │
└───────────────────────┬─────────────────────────────────────────┘
                        │ Analytical views — reporting layer
┌───────────────────────▼─────────────────────────────────────────┐
│  REPORTING schema                                               │
│  RPT_REVENUE_BY_BRAND · RPT_SALES_BY_REGION                    │
│  RPT_MODEL_PROFITABILITY · RPT_SUPPLIER_PERFORMANCE            │
│  RPT_MONTHLY_TREND · RPT_PARTS_COST_BY_SEGMENT                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## Key design decisions

**Surrogate keys over natural keys** — All dimension tables use `AUTOINCREMENT` surrogate keys, decoupling the warehouse from source system ID changes and enabling SCD patterns. This is a deliberate AE choice: the warehouse should be immune to upstream rekeys.

**MERGE for idempotency** — All Gold loads use `MERGE INTO ... USING ... ON ...` so every script can be re-run safely without creating duplicates. This is a production-grade ETL requirement — a failed run should never corrupt the warehouse.

**Silver as views, not tables** — The Silver layer uses `CREATE OR REPLACE VIEW` rather than materialised tables, keeping storage minimal while ensuring Silver always reflects the latest source data. In a high-volume production pipeline this would switch to Dynamic Tables or dbt incremental models.

**DIM_TIME generated, not derived** — The date dimension is pre-populated using Snowflake's `GENERATOR` function, ensuring every date exists in the dimension even if no sales occurred that day. This prevents silent gaps in time-series analysis.

**Gross margin pre-computed in fact** — `gross_margin` and `gross_margin_pct` are stored as measures in the fact table rather than computed at query time, following Kimball's convention for additive measures. This trades storage for query speed and correctness.

**Supplier resolved via latest delivery** — Since there is no direct FK between SALES and SUPPLIER in the source schema, the ETL uses `ROW_NUMBER() OVER (PARTITION BY sale_id ORDER BY delivery_date DESC)` to link each sale to the most recent prior delivery — a common real-world data linkage pattern requiring AE judgement.

---

## From business question to SQL

**Question:** Which car models are losing margin, and how fast?

```sql
-- Month-over-month margin trend by model — using window functions on the reporting layer
SELECT
    brand_model,
    year_month,
    avg_margin_pct,
    avg_margin_pct - LAG(avg_margin_pct) OVER (
        PARTITION BY brand_model ORDER BY year_month
    ) AS margin_delta_pct
FROM CAR_MANUFACTURING_DB.REPORTING.RPT_MODEL_PROFITABILITY
ORDER BY brand_model, year_month;
```

**Insight:** A negative `margin_delta_pct` for 3+ consecutive months is an early warning signal for pricing or supply cost issues — exactly the kind of question the reporting layer is designed to answer in one query.

---

## Project structure

```
car-manufacturing-de-project/
├── README.md
├── 01_source_schema/
│   └── ddl_source.sql                   Original 8 OLTP tables
├── 02_bronze/
│   └── seed_data.sql                    Sample data (30 sales, 20 parts, 10 cars)
├── 03_silver/
│   └── quality_checks_and_staging.sql   6 QC assertion checks + 8 staging views
├── 04_gold/
│   ├── ddl_dimensions.sql               DIM_TIME, DIM_CAR, DIM_CUSTOMER, DIM_SUPPLIER, DIM_PART
│   ├── ddl_fact.sql                     FACT_SALES with pre-computed margin measures
│   └── etl_load_gold.sql                MERGE-based idempotent load: Silver → Gold
├── 05_reporting/
│   └── views.sql                        6 analytical views ready for BI connection
└── docs/
    ├── architecture.svg                 Architecture diagram
    ├── data_dictionary.md               Field definitions, business rules, lineage
    └── dbdiagram_schema.dbml            Star schema for dbdiagram.io
```

---

## How to run

You need a Snowflake account (the [30-day free trial](https://signup.snowflake.com/) works). Execute scripts in order inside a Snowflake worksheet:

```sql
-- 1. Create source schema and OLTP tables
-- Run: 01_source_schema/ddl_source.sql

-- 2. Load sample data
-- Run: 02_bronze/seed_data.sql

-- 3. Run quality checks (all should return 0 failures), then create Silver views
-- Run: 03_silver/quality_checks_and_staging.sql

-- 4. Create dimension tables + auto-generate DIM_TIME (2022–2025)
-- Run: 04_gold/ddl_dimensions.sql

-- 5. Create FACT_SALES
-- Run: 04_gold/ddl_fact.sql

-- 6. Load all dimensions and fact via idempotent MERGE
-- Run: 04_gold/etl_load_gold.sql

-- 7. Create reporting views
-- Run: 05_reporting/views.sql

-- 8. Query the reporting layer
SELECT * FROM CAR_MANUFACTURING_DB.REPORTING.RPT_REVENUE_BY_BRAND;
SELECT * FROM CAR_MANUFACTURING_DB.REPORTING.RPT_MODEL_PROFITABILITY ORDER BY avg_margin_pct DESC;
SELECT * FROM CAR_MANUFACTURING_DB.REPORTING.RPT_MONTHLY_TREND;
```

---

## Sample queries

```sql
-- Top 3 most profitable car models
SELECT brand_model, avg_margin_pct, units_sold, total_gross_margin
FROM CAR_MANUFACTURING_DB.REPORTING.RPT_MODEL_PROFITABILITY
ORDER BY avg_margin_pct DESC
LIMIT 3;

-- Month-over-month revenue growth in 2023
SELECT year_month, total_revenue, mom_revenue_growth_pct
FROM CAR_MANUFACTURING_DB.REPORTING.RPT_MONTHLY_TREND
WHERE year = 2023
ORDER BY month;

-- Revenue breakdown by region
SELECT region, SUM(total_revenue) AS revenue
FROM CAR_MANUFACTURING_DB.REPORTING.RPT_SALES_BY_REGION
GROUP BY region
ORDER BY revenue DESC;
```

---

## Technologies

![Snowflake](https://img.shields.io/badge/-Snowflake-29B5E8?logo=snowflake&logoColor=white&style=flat)
![SQL](https://img.shields.io/badge/-SQL-4479A1?logo=postgresql&logoColor=white&style=flat)
![dbt](https://img.shields.io/badge/-dbt%20compatible-FF694B?logo=dbt&logoColor=white&style=flat)

- **Snowflake** — Cloud data warehouse. All SQL uses Snowflake dialect.
- **SQL** — DDL, DML, window functions (`LAG`, `ROW_NUMBER`, `PARTITION BY`), CTEs, MERGE
- **Kimball dimensional modelling** — Star schema, surrogate keys, SCD Type 1 ready
- **Medallion architecture** — Bronze / Silver / Gold / Reporting data layering
- **dbt-compatible architecture** — Silver views and Gold MERGE loads map directly to dbt models and incremental strategies; migration path is straightforward

---

## Possible extensions

- Add **dbt models** to replace the manual Silver views and Gold MERGE scripts — natural next step for a production AE setup
- Add a **Snowflake Task** to schedule the ETL on a daily cron
- Implement **Snowflake Streams** for CDC (Change Data Capture) instead of full MERGE loads
- Add **SCD Type 2** to `DIM_CUSTOMER` to track address changes over time
- Connect a **Streamlit in Snowflake** or **Metabase** dashboard on top of the reporting views

---

## Author

**Sergi de la Cruz Núñez**
[LinkedIn](https://www.linkedin.com/in/sergi-de-la-cruz-905543257/) · [sergidelacruz1994@gmail.com](mailto:sergidelacruz1994@gmail.com)
