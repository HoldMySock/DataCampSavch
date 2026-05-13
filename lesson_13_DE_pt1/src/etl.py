"""
ETL Pipeline — cleans raw CSVs, loads into SQLite, creates analytical tables.
Run: python src/etl.py
"""

import re
import sqlite3
import logging
import pandas as pd
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
log = logging.getLogger(__name__)

RAW = Path(__file__).parent.parent / "raw_data"
DB  = Path(__file__).parent.parent / "output" / "store.db"

def _valid_email(email: str) -> bool:
    return bool(re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", str(email)))


#cleaning

def clean_customers(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    original = len(df)

    # 1. Drop rows where customer_id is missing
    missing_id = df["customer_id"].isna().sum()
    df = df.dropna(subset=["customer_id"])
    log.info(f"customers: dropped {missing_id} rows with missing customer_id")

    # 2. Format customer_id to int
    df["customer_id"] = df["customer_id"].astype(int)

    # 3. Remove duplicate customer_ids
    dupes = df["customer_id"].duplicated().sum()
    df = df.drop_duplicates(subset=["customer_id"], keep="first")
    log.info(f"customers: removed {dupes} duplicate customer_id rows")

    # 4. Set invalid or missing emails to NULL
    invalid_mask = df["email"].isna() | ~df["email"].apply(
        lambda e: _valid_email(e) if pd.notna(e) else False
    )
    n_bad = invalid_mask.sum()
    df.loc[invalid_mask, "email"] = None
    log.info(f"customers: nulled {n_bad} missing/invalid emails")

    # 5. Parse created_at
    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")
    bad_ts = df["created_at"].isna().sum()
    log.info(f"customers: {bad_ts} unparseable created_at timestamps set to NULL")

    log.info(f"customers: {original} → {len(df)} rows")
    return df


def clean_products(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    original = len(df)

    # 1. Remove duplicate product_ids
    dupes = df["product_id"].duplicated().sum()
    df = df.drop_duplicates(subset=["product_id"], keep="first")
    log.info(f"products: removed {dupes} duplicate product_id rows")

    # 2. Drop rows with negative price
    bad_price = (df["price"] <= 0).sum()
    df = df[df["price"] > 0]
    log.info(f"products: dropped {bad_price} rows with zero/negative price")

    # 3. Fill missing name / category with placeholder
    df["name"]     = df["name"].fillna("Unknown Product")
    df["category"] = df["category"].fillna("Unknown Category")
    log.info("products: filled NULL name/category with placeholder strings")

    log.info(f"products: {original} → {len(df)} rows")
    return df


def clean_orders(path: Path, valid_customer_ids: set) -> pd.DataFrame:
    df = pd.read_csv(path)
    original = len(df)

    # 1. Remove duplicate order_ids
    dupes = df["order_id"].duplicated().sum()
    df = df.drop_duplicates(subset=["order_id"], keep="first")
    log.info(f"orders: removed {dupes} duplicate order_id rows")

    # 2. Drop rows with missing customer_id
    missing = df["customer_id"].isna().sum()
    df = df.dropna(subset=["customer_id"])
    df["customer_id"] = df["customer_id"].astype(int)
    log.info(f"orders: dropped {missing} rows with missing customer_id")

    # 3. Normalise status to lowercase
    df["order_status"] = df["order_status"].str.strip().str.lower()

    # 4. Log unknown statuses
    known = {"pending", "completed", "cancelled", "returned"}
    unknown_mask = ~df["order_status"].isin(known)
    n_unknown = unknown_mask.sum()
    if n_unknown:
        vals = df.loc[unknown_mask, "order_status"].unique().tolist()
        df.loc[unknown_mask, "order_status"] = "unknown"
        log.info(f"orders: replaced {n_unknown} unknown statuses {vals} with 'unknown'")
    else:
        log.info("orders: all statuses valid after normalisation")

    # 5. Drop orders referencing a customer_id not in customers table
    orphan_mask = ~df["customer_id"].isin(valid_customer_ids)
    n_orphan = orphan_mask.sum()
    df = df[~orphan_mask]
    log.info(f"orders: dropped {n_orphan} orders with customer_id not in customers table")

    # 6. Parse created_at
    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")
    bad_ts = df["created_at"].isna().sum()
    log.info(f"orders: {bad_ts} unparseable created_at timestamps set to NULL")

    log.info(f"orders: {original} → {len(df)} rows")
    return df


def clean_order_items(
    path: Path, valid_order_ids: set, valid_product_ids: set
) -> pd.DataFrame:
    df = pd.read_csv(path)
    original = len(df)

    # 1. Remove duplicate order_item_ids
    dupes = df["order_item_id"].duplicated().sum()
    df = df.drop_duplicates(subset=["order_item_id"], keep="first")
    log.info(f"order_items: removed {dupes} duplicate order_item_id rows")

    # 2. Drop rows with negativee quantity
    bad_qty = (df["quantity"] <= 0).sum()
    df = df[df["quantity"] > 0]
    log.info(f"order_items: dropped {bad_qty} rows with zero/negative quantity")

    # 3. Drop rows referencing orders not in orders table
    orphan_orders = ~df["order_id"].isin(valid_order_ids)
    n = orphan_orders.sum()
    df = df[~orphan_orders]
    log.info(f"order_items: dropped {n} rows with order_id not in orders table")

    # 4. Drop rows referencing products not in products table
    orphan_prods = ~df["product_id"].isin(valid_product_ids)
    n = orphan_prods.sum()
    df = df[~orphan_prods]
    log.info(f"order_items: dropped {n} rows with product_id not in products table")

    log.info(f"order_items: {original} → {len(df)} rows")
    return df


#analytical tables, made with ai help

def build_analytical_tables(conn: sqlite3.Connection) -> None:
    """Create reporting-ready views / materialised tables."""

    # 1. order_summary — one row per order with total revenue and item count
    conn.execute("DROP TABLE IF EXISTS order_summary")
    conn.execute("""
        CREATE TABLE order_summary AS
        SELECT
            o.order_id,
            o.customer_id,
            o.order_status,
            o.created_at                          AS order_date,
            COUNT(oi.order_item_id)               AS line_items,
            SUM(oi.quantity)                      AS total_units,
            ROUND(SUM(oi.quantity * p.price), 2)  AS total_revenue
        FROM orders o
        JOIN order_items oi ON oi.order_id    = o.order_id
        JOIN products    p  ON p.product_id   = oi.product_id
        GROUP BY o.order_id
    """)
    log.info("analytical: created order_summary")

    # 2. customer_stats — lifetime value + order count per customer
    conn.execute("DROP TABLE IF EXISTS customer_stats")
    conn.execute("""
        CREATE TABLE customer_stats AS
        SELECT
            c.customer_id,
            c.email,
            c.country,
            c.created_at                              AS registered_at,
            COUNT(DISTINCT o.order_id)                AS total_orders,
            COALESCE(SUM(os.total_revenue), 0)        AS lifetime_value,
            MIN(o.created_at)                         AS first_order_date,
            MAX(o.created_at)                         AS last_order_date
        FROM customers c
        LEFT JOIN orders       o  ON o.customer_id = c.customer_id
        LEFT JOIN order_summary os ON os.order_id   = o.order_id
        GROUP BY c.customer_id
    """)
    log.info("analytical: created customer_stats")

    # 3. product_performance — units sold, revenue, order count per product
    conn.execute("DROP TABLE IF EXISTS product_performance")
    conn.execute("""
        CREATE TABLE product_performance AS
        SELECT
            p.product_id,
            p.name,
            p.category,
            p.price                                   AS unit_price,
            COUNT(DISTINCT oi.order_id)               AS orders_containing,
            SUM(oi.quantity)                          AS total_units_sold,
            ROUND(SUM(oi.quantity * p.price), 2)      AS total_revenue
        FROM products p
        LEFT JOIN order_items oi ON oi.product_id = p.product_id
        LEFT JOIN orders       o  ON o.order_id   = oi.order_id
                                 AND o.order_status NOT IN ('cancelled', 'returned')
        GROUP BY p.product_id
    """)
    log.info("analytical: created product_performance")

    # 4. monthly_revenue — completed/pending revenue by month
    conn.execute("DROP TABLE IF EXISTS monthly_revenue")
    conn.execute("""
        CREATE TABLE monthly_revenue AS
        SELECT
            STRFTIME('%Y-%m', o.created_at)       AS month,
            o.order_status,
            COUNT(DISTINCT o.order_id)            AS orders,
            ROUND(SUM(os.total_revenue), 2)       AS revenue
        FROM orders        o
        JOIN order_summary os ON os.order_id = o.order_id
        WHERE o.order_status IN ('completed', 'pending')
        GROUP BY month, o.order_status
        ORDER BY month
    """)
    log.info("analytical: created monthly_revenue")

    # 5. category_summary — revenue + units per product category
    conn.execute("DROP TABLE IF EXISTS category_summary")
    conn.execute("""
        CREATE TABLE category_summary AS
        SELECT
            p.category,
            COUNT(DISTINCT p.product_id)              AS products,
            SUM(oi.quantity)                          AS total_units_sold,
            ROUND(SUM(oi.quantity * p.price), 2)      AS total_revenue
        FROM products    p
        JOIN order_items oi ON oi.product_id = p.product_id
        JOIN orders       o ON o.order_id    = oi.order_id
                            AND o.order_status NOT IN ('cancelled', 'returned')
        GROUP BY p.category
        ORDER BY total_revenue DESC
    """)
    log.info("analytical: created category_summary")

    conn.commit()


# main

def run():
    log.info("Starting ETL pipeline")

    #clean
    customers  = clean_customers(RAW / "customers.csv")
    products   = clean_products(RAW / "products.csv")
    orders     = clean_orders(RAW / "orders.csv",
                              valid_customer_ids=set(customers["customer_id"]))
    order_items = clean_order_items(
        RAW / "order_items.csv",
        valid_order_ids  = set(orders["order_id"]),
        valid_product_ids= set(products["product_id"]),
    )

    #load
    DB.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB)
    customers.to_sql("customers",   conn, if_exists="replace", index=False)
    products.to_sql("products",     conn, if_exists="replace", index=False)
    orders.to_sql("orders",         conn, if_exists="replace", index=False)
    order_items.to_sql("order_items", conn, if_exists="replace", index=False)
    log.info("Loaded 4 cleaned tables into SQLite")

    #analytical
    build_analytical_tables(conn)

    #quick sanity print
    log.info("Pipeline complete")
    for tbl in ("order_summary","customer_stats","product_performance",
                "monthly_revenue","category_summary"):
        n = conn.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
        log.info(f"  {tbl}: {n} rows")

    conn.close()


if __name__ == "__main__":
    run()
