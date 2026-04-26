"""
=============================================================================
Car Manufacturing Supply Control — Bronze Layer: Python Ingestion
=============================================================================
Description:
    Generates realistic synthetic data using Faker and pandas, then loads
    it directly into Snowflake SOURCE tables via the Snowflake Python Connector.

    This script replaces the static seed_data.sql for a more realistic
    pipeline simulation. It can be re-run safely — all loads are truncate+insert
    so results are always deterministic.

Usage:
    1. Install dependencies:  pip install -r requirements.txt
    2. Set your credentials:  copy .env.example to .env and fill in values
    3. Run:                   python ingest_bronze.py

    Optional flags:
        --rows-sales     Number of sales records to generate (default: 100)
        --rows-customers Number of customers to generate (default: 50)
        --dry-run        Generate data and print to console, skip Snowflake load
=============================================================================
"""

import argparse
import logging
import os
import random
from datetime import date, timedelta

import pandas as pd
from dotenv import load_dotenv
from faker import Faker
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

# ---------------------------------------------------------------------------
# Config & logging
# ---------------------------------------------------------------------------
load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

fake = Faker("en_GB")
Faker.seed(42)          # reproducible data across runs
random.seed(42)

# ---------------------------------------------------------------------------
# Static reference data (matches DDL in 01_source_schema/ddl_source.sql)
# ---------------------------------------------------------------------------

CATEGORIES = [
    (1, "Engine"),
    (2, "Bodywork"),
    (3, "Electronics"),
    (4, "Suspension"),
    (5, "Interior"),
    (6, "Brakes"),
]

PARTS = [
    (101, "Cylinder Block",        "Grey",   850.00, 1),
    (102, "Crankshaft",            "Silver", 320.00, 1),
    (103, "Front Bumper",          "White",  210.00, 2),
    (104, "Rear Bumper",           "White",  195.00, 2),
    (105, "Side Door Panel Left",  "Black",  430.00, 2),
    (106, "Side Door Panel Right", "Black",  430.00, 2),
    (107, "ECU Control Unit",      "Black",  680.00, 3),
    (108, "Infotainment Screen",   "Black",  540.00, 3),
    (109, "Front Suspension Kit",  "Grey",   760.00, 4),
    (110, "Rear Suspension Kit",   "Grey",   710.00, 4),
    (111, "Leather Seat Set",      "Beige",  920.00, 5),
    (112, "Dashboard Assembly",    "Black",  380.00, 5),
    (113, "Disc Brake Front",      "Silver", 290.00, 6),
    (114, "Disc Brake Rear",       "Silver", 260.00, 6),
    (115, "Exhaust Manifold",      "Grey",   175.00, 1),
    (116, "Hood Panel",            "White",  340.00, 2),
    (117, "Wiring Harness",        "Black",  220.00, 3),
    (118, "Shock Absorber Front",  "Black",  180.00, 4),
    (119, "Steering Wheel",        "Black",  130.00, 5),
    (120, "ABS Module",            "Silver", 310.00, 6),
]

CARS = [
    (1,  "SEAT",       "Ibiza",     2022, 18500),
    (2,  "SEAT",       "Leon",      2022, 24500),
    (3,  "SEAT",       "Arona",     2023, 22000),
    (4,  "Volkswagen", "Golf",      2023, 32000),
    (5,  "Volkswagen", "Polo",      2022, 20000),
    (6,  "Audi",       "A3",        2023, 38000),
    (7,  "Audi",       "Q3",        2023, 45000),
    (8,  "CUPRA",      "Formentor", 2023, 42500),
    (9,  "CUPRA",      "Born",      2023, 39000),
    (10, "Volkswagen", "Tiguan",    2024, 48000),
]

# Bill of materials: car_id → list of (part_id, quantity)
BOM = {
    1:  [(101,1),(102,1),(103,1),(104,1),(107,1),(109,1),(110,1),(111,1),(113,2),(114,2)],
    2:  [(101,1),(102,1),(103,1),(105,1),(106,1),(107,1),(108,1),(109,1),(110,1),(111,1),(113,2),(114,2)],
    3:  [(101,1),(103,1),(104,1),(107,1),(108,1),(109,1),(110,1),(111,1),(113,2),(116,1)],
    4:  [(101,1),(102,1),(103,1),(104,1),(105,1),(106,1),(107,1),(108,1),(109,1),(110,1),(111,1),(112,1),(113,2),(114,2)],
    5:  [(101,1),(102,1),(103,1),(107,1),(109,1),(110,1),(111,1),(113,2),(114,2)],
    6:  [(101,1),(102,1),(103,1),(104,1),(105,1),(106,1),(107,1),(108,1),(109,1),(110,1),(111,1),(112,1),(113,2),(114,2),(120,1)],
    7:  [(101,1),(102,1),(103,1),(104,1),(105,1),(106,1),(107,1),(108,1),(109,1),(110,1),(111,1),(112,1),(113,2),(114,2),(116,1),(120,1)],
    8:  [(101,1),(102,1),(103,1),(104,1),(105,1),(106,1),(107,1),(108,1),(109,1),(110,1),(111,1),(112,1),(113,2),(114,2),(117,1),(120,1)],
    9:  [(107,1),(108,1),(109,1),(110,1),(111,1),(112,1),(113,2),(114,2),(117,1),(119,1),(120,1)],
    10: [(101,1),(102,1),(103,1),(104,1),(105,1),(106,1),(107,1),(108,1),(109,1),(110,1),(111,1),(112,1),(113,2),(114,2),(116,1),(118,2),(120,1)],
}

PROVINCES = [
    ("Barcelona", "Catalonia"), ("Girona", "Catalonia"),
    ("Madrid", "Madrid"),       ("Valencia", "Valencia"),
    ("Seville", "Andalusia"),   ("Malaga", "Andalusia"),
    ("Bilbao", "Basque Country"),("Zaragoza", "Aragon"),
    ("Alicante", "Valencia"),   ("Cordoba", "Andalusia"),
]

# ---------------------------------------------------------------------------
# Data generators
# ---------------------------------------------------------------------------

def generate_suppliers(n: int = 10) -> pd.DataFrame:
    """Generate supplier records."""
    log.info("Generating %d suppliers...", n)
    rows = []
    industries = ["Auto Parts", "Metal Components", "Electro Systems",
                  "Precision Parts", "Steel Works", "Rubber Tech",
                  "Glass Solutions", "Plastic Innovations", "Cable Systems", "Forge Industries"]
    suffixes = ["S.L.", "S.A.", "GmbH", "Ltd.", "Inc.", "B.V."]
    province_cities = {p: c for c, p in PROVINCES}

    for i in range(1, n + 1):
        province, _ = random.choice(PROVINCES)
        rows.append({
            "PROVEEDOR_ID": i,
            "NOMBRE":       f"{random.choice(industries)} {random.choice(suffixes)}",
            "DIRECCION":    f"{random.randint(1,200)} {fake.street_name()}",
            "CIUDAD":       province_cities.get(province, fake.city()),
            "PROVINCIA":    province,
        })
    return pd.DataFrame(rows)


def generate_customers(n: int = 50) -> pd.DataFrame:
    """Generate customer records."""
    log.info("Generating %d customers...", n)
    rows = []
    for i in range(1, n + 1):
        city, province = random.choice(PROVINCES)
        rows.append({
            "CLIENTE_ID": i,
            "NOMBRE":     fake.name(),
            "DIRECCION":  f"{random.randint(1,300)} {fake.street_name()}",
            "CIUDAD":     city,
            "PROVINCIA":  province,
        })
    return pd.DataFrame(rows)


def generate_categories() -> pd.DataFrame:
    """Return static category reference data as DataFrame."""
    log.info("Generating categories...")
    return pd.DataFrame(CATEGORIES, columns=["CATEGORY_ID", "NOMBRE"])


def generate_parts() -> pd.DataFrame:
    """Return static parts reference data as DataFrame."""
    log.info("Generating parts...")
    return pd.DataFrame(PARTS, columns=["PIEZA_ID", "NOMBRE", "COLOR", "PRECIO", "CATEGORY_ID"])


def generate_cars() -> pd.DataFrame:
    """Return static car models as DataFrame (base_price used for sales generation)."""
    log.info("Generating cars...")
    df = pd.DataFrame(CARS, columns=["CAR_ID", "MARCA", "MODELO", "ANO", "BASE_PRICE"])
    return df.drop(columns=["BASE_PRICE"])  # base_price is not in source schema


def generate_parts_car() -> pd.DataFrame:
    """Generate bill of materials from static BOM dict."""
    log.info("Generating parts_car (BOM)...")
    rows = []
    for car_id, parts in BOM.items():
        for part_id, qty in parts:
            rows.append({"CAR_ID": car_id, "PIEZA_ID": part_id, "CANTIDAD_PIEZA": qty})
    return pd.DataFrame(rows)


def generate_deliveries(n_deliveries: int = 50, n_suppliers: int = 10) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Generate final_delivery and lot_delivery records.
    Returns a tuple: (final_delivery_df, lot_delivery_df)
    """
    log.info("Generating %d deliveries...", n_deliveries)
    start_date = date(2023, 1, 1)
    end_date   = date(2024, 6, 30)
    date_range = (end_date - start_date).days

    final_deliveries = []
    lot_deliveries   = []
    part_ids         = [p[0] for p in PARTS]

    for i in range(1, n_deliveries + 1):
        delivery_date = start_date + timedelta(days=random.randint(0, date_range))
        supplier_id   = random.randint(1, n_suppliers)
        entrega_id    = 1000 + i

        final_deliveries.append({
            "ENTREGA_ID":    entrega_id,
            "PROVEEDOR_ID":  supplier_id,
            "FECHA":         delivery_date.isoformat(),
        })

        # Each delivery contains 2–5 random parts
        selected_parts = random.sample(part_ids, k=random.randint(2, 5))
        for part_id in selected_parts:
            lot_deliveries.append({
                "ENTREGA_ID": entrega_id,
                "PIEZA_ID":   part_id,
                "FECHA":      delivery_date.isoformat(),
                "CANTIDAD":   random.randint(20, 200),
            })

    return pd.DataFrame(final_deliveries), pd.DataFrame(lot_deliveries)


def generate_sales(n: int = 100, n_customers: int = 50) -> pd.DataFrame:
    """
    Generate sales records.
    Price is base_price ± 10% random variation to simulate negotiation.
    """
    log.info("Generating %d sales...", n)
    start_date = date(2023, 1, 1)
    end_date   = date(2024, 6, 30)
    date_range = (end_date - start_date).days

    car_prices = {c[0]: c[4] for c in CARS}  # car_id → base_price
    rows = []

    for i in range(1, n + 1):
        car_id     = random.randint(1, 10)
        base_price = car_prices[car_id]
        variation  = random.uniform(-0.10, 0.10)
        sale_price = round(base_price * (1 + variation), 2)
        sale_date  = start_date + timedelta(days=random.randint(0, date_range))

        rows.append({
            "VENTA_ID":   i,
            "CLIENTE_ID": random.randint(1, n_customers),
            "CAR_ID":     car_id,
            "FECHA":      sale_date.isoformat(),
            "PRECIO":     sale_price,
        })

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Snowflake connection
# ---------------------------------------------------------------------------

def get_snowflake_connection():
    """
    Build Snowflake connection from environment variables.
    Required env vars (set in .env file):
        SNOWFLAKE_ACCOUNT   — e.g. xy12345.eu-west-1
        SNOWFLAKE_USER      — your Snowflake username
        SNOWFLAKE_PASSWORD  — your Snowflake password
        SNOWFLAKE_DATABASE  — CAR_MANUFACTURING_DB
        SNOWFLAKE_SCHEMA    — SOURCE
        SNOWFLAKE_WAREHOUSE — your warehouse name (e.g. COMPUTE_WH)
        SNOWFLAKE_ROLE      — optional, defaults to your user's default role
    """
    log.info("Connecting to Snowflake account: %s", os.getenv("SNOWFLAKE_ACCOUNT"))
    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        database=os.environ.get("SNOWFLAKE_DATABASE", "CAR_MANUFACTURING_DB"),
        schema=os.environ.get("SNOWFLAKE_SCHEMA", "SOURCE"),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        role=os.environ.get("SNOWFLAKE_ROLE", ""),
    )
    log.info("Connected successfully.")
    return conn


# ---------------------------------------------------------------------------
# Loader
# ---------------------------------------------------------------------------

def load_table(conn, df: pd.DataFrame, table_name: str) -> None:
    """
    Truncate target table and bulk-load a DataFrame using write_pandas.
    write_pandas uses Snowflake's COPY INTO under the hood — much faster
    than row-by-row inserts for large volumes.
    """
    log.info("Loading %d rows into %s...", len(df), table_name)

    # Truncate first for idempotency
    cur = conn.cursor()
    cur.execute(f"TRUNCATE TABLE IF EXISTS {table_name}")
    cur.close()

    success, n_chunks, n_rows, _ = write_pandas(
        conn=conn,
        df=df,
        table_name=table_name,
        auto_create_table=False,
        overwrite=False,
    )

    if success:
        log.info("  ✓ %d rows loaded into %s (%d chunk(s))", n_rows, table_name, n_chunks)
    else:
        log.error("  ✗ Failed to load %s", table_name)
        raise RuntimeError(f"write_pandas failed for table {table_name}")


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def run(args):
    # 1 — Generate all DataFrames
    df_suppliers     = generate_suppliers(n=args.rows_suppliers)
    df_customers     = generate_customers(n=args.rows_customers)
    df_categories    = generate_categories()
    df_parts         = generate_parts()
    df_cars          = generate_cars()
    df_parts_car     = generate_parts_car()
    df_deliveries, df_lots = generate_deliveries(
        n_deliveries=args.rows_deliveries,
        n_suppliers=args.rows_suppliers,
    )
    df_sales         = generate_sales(
        n=args.rows_sales,
        n_customers=args.rows_customers,
    )

    # Summary
    log.info("--- Data generation complete ---")
    for name, df in [
        ("SUPPLIER", df_suppliers), ("CUSTOMERS", df_customers),
        ("CATEGORY", df_categories), ("PARTS", df_parts),
        ("CARS", df_cars), ("PARTS_CAR", df_parts_car),
        ("FINAL_DELIVERY", df_deliveries), ("LOT_DELIVERY", df_lots),
        ("SALES", df_sales),
    ]:
        log.info("  %-20s %d rows", name, len(df))

    if args.dry_run:
        log.info("Dry run — skipping Snowflake load. Sample output:")
        print("\n--- SALES sample ---")
        print(df_sales.head(5).to_string(index=False))
        print("\n--- CUSTOMERS sample ---")
        print(df_customers.head(5).to_string(index=False))
        return

    # 2 — Load into Snowflake
    conn = get_snowflake_connection()
    try:
        # Order matters — respect FK dependencies
        load_table(conn, df_categories,  "CATEGORY")
        load_table(conn, df_parts,        "PARTS")
        load_table(conn, df_cars,         "CARS")
        load_table(conn, df_parts_car,    "PARTS_CAR")
        load_table(conn, df_suppliers,    "SUPPLIER")
        load_table(conn, df_customers,    "CUSTOMERS")
        load_table(conn, df_deliveries,   "FINAL_DELIVERY")
        load_table(conn, df_lots,         "LOT_DELIVERY")
        load_table(conn, df_sales,        "SALES")
        log.info("--- All tables loaded successfully ---")
    finally:
        conn.close()
        log.info("Snowflake connection closed.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Bronze ingestion — generate synthetic data and load into Snowflake SOURCE schema."
    )
    parser.add_argument("--rows-sales",      type=int, default=100, help="Number of sales records (default: 100)")
    parser.add_argument("--rows-customers",  type=int, default=50,  help="Number of customer records (default: 50)")
    parser.add_argument("--rows-suppliers",  type=int, default=10,  help="Number of supplier records (default: 10)")
    parser.add_argument("--rows-deliveries", type=int, default=50,  help="Number of delivery records (default: 50)")
    parser.add_argument("--dry-run",         action="store_true",   help="Generate data but skip Snowflake load")

    args = parser.parse_args()
    run(args)
