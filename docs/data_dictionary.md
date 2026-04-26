# Data Dictionary — Car Manufacturing Supply Control

**Database:** `CAR_MANUFACTURING_DB`  
**Last updated:** 2024-04  
**Owner:** Data Engineering Team

---

## Schema overview

| Schema | Layer | Purpose |
|--------|-------|---------|
| `SOURCE` | Raw | Original OLTP tables. Never modified. |
| `SILVER` | Staging | Cleansed views with English naming and type casting. |
| `GOLD` | Warehouse | Star schema — dimensions + fact table. |
| `REPORTING` | Serving | Analytical views for BI tools. |

---

## Gold Layer — Dimension Tables

### `DIM_TIME`

Date dimension. Grain: one row per calendar day (2022–2025).

| Column | Type | Description |
|--------|------|-------------|
| `time_id` | INTEGER PK | YYYYMMDD integer — e.g. `20230115` |
| `full_date` | DATE | Calendar date |
| `year` | INTEGER | 4-digit year |
| `quarter` | INTEGER | 1–4 |
| `quarter_name` | VARCHAR(6) | 'Q1' … 'Q4' |
| `month` | INTEGER | 1–12 |
| `month_name` | VARCHAR(20) | 'January' … 'December' |
| `month_short` | VARCHAR(3) | 'Jan' … 'Dec' |
| `week_of_year` | INTEGER | ISO week number |
| `day_of_month` | INTEGER | 1–31 |
| `day_of_week` | INTEGER | 0 = Sunday, 6 = Saturday |
| `day_name` | VARCHAR(10) | 'Monday' … 'Sunday' |
| `is_weekend` | BOOLEAN | TRUE if Saturday or Sunday |

---

### `DIM_CUSTOMER`

End-customer profiles. SCD Type 1 (overwrite on change).

| Column | Type | Description |
|--------|------|-------------|
| `customer_key` | INTEGER PK | Surrogate key (auto-increment) |
| `customer_id` | INTEGER | Source natural key from `CUSTOMERS.Cliente_ID` |
| `customer_name` | NVARCHAR(100) | Full name |
| `address` | NVARCHAR(200) | Street address. Defaults to `'Unknown'` if NULL in source. |
| `city` | NVARCHAR(100) | City. Defaults to `'Unknown'` if NULL. |
| `province` | NVARCHAR(100) | Spanish province |
| `region` | VARCHAR(50) | Derived grouping based on province: `Catalonia`, `Madrid`, `Valencia`, `Andalusia`, `Basque Country`, `Other` |
| `load_ts` | TIMESTAMP_NTZ | ETL load timestamp |

---

### `DIM_CAR`

Car model catalogue, enriched with bill-of-materials counts.

| Column | Type | Description |
|--------|------|-------------|
| `car_key` | INTEGER PK | Surrogate key |
| `car_id` | INTEGER | Source natural key from `CARS.Car_ID` |
| `brand` | TEXT | Manufacturer (e.g. `SEAT`, `Audi`) |
| `model` | TEXT | Model name (e.g. `Ibiza`, `Q3`) |
| `manufacture_year` | INTEGER | Model year |
| `brand_model` | TEXT | Derived: `brand \|\| ' ' \|\| model` — e.g. `SEAT Ibiza` |
| `segment` | VARCHAR(30) | Derived classification: `Electric`, `SUV`, `Compact`, `Subcompact`, `Other` |
| `unique_parts` | INTEGER | Count of distinct parts in BOM |
| `total_parts_qty` | INTEGER | Sum of all part quantities across BOM |
| `load_ts` | TIMESTAMP_NTZ | ETL load timestamp |

---

### `DIM_SUPPLIER`

Parts suppliers.

| Column | Type | Description |
|--------|------|-------------|
| `supplier_key` | INTEGER PK | Surrogate key |
| `supplier_id` | INTEGER | Source natural key from `SUPPLIER.Proveedor_ID` |
| `supplier_name` | NVARCHAR(100) | Company name |
| `address` | NVARCHAR(200) | Street address |
| `city` | NVARCHAR(100) | City |
| `province` | NVARCHAR(100) | Spanish province |
| `load_ts` | TIMESTAMP_NTZ | ETL load timestamp |

---

### `DIM_PART`

Individual parts catalogue. Category is denormalised in to avoid joins at query time.

| Column | Type | Description |
|--------|------|-------------|
| `part_key` | INTEGER PK | Surrogate key |
| `part_id` | INTEGER | Source natural key from `PARTS.Pieza_ID` |
| `part_name` | TEXT | Part description (e.g. `Cylinder Block`) |
| `color` | TEXT | Part colour. Defaults to `'Unknown'` if NULL. |
| `unit_price` | DECIMAL(10,2) | Purchase price per unit in EUR |
| `category_id` | INTEGER | FK to source category — kept for traceability |
| `category_name` | TEXT | Denormalised from `CATEGORY.Nombre` (e.g. `Engine`, `Bodywork`) |
| `load_ts` | TIMESTAMP_NTZ | ETL load timestamp |

---

## Gold Layer — Fact Table

### `FACT_SALES`

**Grain:** One row per car sale transaction.  
**Source:** `SALES` table joined to BOM and delivery data.

| Column | Type | Description |
|--------|------|-------------|
| `sale_key` | INTEGER PK | Surrogate key |
| `car_key` | INTEGER FK | → `DIM_CAR.car_key` |
| `customer_key` | INTEGER FK | → `DIM_CUSTOMER.customer_key` |
| `supplier_key` | INTEGER FK | → `DIM_SUPPLIER.supplier_key` — most recent supplier with delivery on/before sale date |
| `time_id` | INTEGER FK | → `DIM_TIME.time_id` (YYYYMMDD of sale date) |
| `sale_id` | INTEGER | Degenerate dimension — source `Venta_ID` for traceability |
| `sale_price` | NUMERIC(12,2) | Final selling price to customer in EUR |
| `total_parts_cost` | DECIMAL(12,2) | Sum of `unit_price × quantity` for all BOM parts of this car model |
| `gross_margin` | DECIMAL(12,2) | `sale_price - total_parts_cost` |
| `gross_margin_pct` | DECIMAL(6,4) | `gross_margin / sale_price` — e.g. `0.4823` = 48.23% |
| `unique_parts_count` | INTEGER | Distinct part types in BOM |
| `total_parts_qty` | INTEGER | Total quantity of all parts across BOM |
| `load_ts` | TIMESTAMP_NTZ | ETL load timestamp |

---

## Business Rules

| Rule ID | Description | Enforced in |
|---------|-------------|-------------|
| BR-01 | `sale_price` must be > 0 | QC-02 in Silver layer |
| BR-02 | `total_parts_cost` must be > 0 | QC-02 in Silver layer |
| BR-03 | `gross_margin` should be positive (sale_price > cost) | QC-06 in Silver layer |
| BR-04 | All FK references must resolve | QC-03 in Silver layer |
| BR-05 | No future sale or delivery dates | QC-04 in Silver layer |
| BR-06 | No duplicate primary keys in source | QC-05 in Silver layer |
| BR-07 | `supplier_key` in FACT is resolved via latest delivery ≤ sale date | ETL logic in `etl_load_gold.sql` |

---

## Lineage

```
SOURCE.SALES
SOURCE.CARS          →  SILVER views (rename, cast, derive)  →  GOLD.DIM_CAR
SOURCE.CUSTOMERS                                              →  GOLD.DIM_CUSTOMER
SOURCE.SUPPLIER                                               →  GOLD.DIM_SUPPLIER
SOURCE.PARTS + CATEGORY                                       →  GOLD.DIM_PART
                                                              →  GOLD.FACT_SALES
                                                              →  REPORTING.RPT_*
```

---

## Reporting Views

| View | Description |
|------|-------------|
| `RPT_REVENUE_BY_BRAND` | Revenue, cost, and margin by brand and quarter |
| `RPT_SALES_BY_REGION` | Sales volume and revenue by Spanish region and month |
| `RPT_MODEL_PROFITABILITY` | Gross margin per car model and segment |
| `RPT_SUPPLIER_PERFORMANCE` | Supplier-linked revenue and cost-to-revenue ratio |
| `RPT_MONTHLY_TREND` | Month-over-month revenue growth time series |
| `RPT_PARTS_COST_BY_SEGMENT` | BOM cost breakdown by part category per car segment |
