import os

os.environ["PREFECT_API_URL"] = "http://127.0.0.1:4201/api"

from prefect import task, flow, get_run_logger
import pandas as pd
import sqlite3
from pathlib import Path


def clean_phone(phone):
    """Remove non-numeric characters from ONE phone value."""
    if phone is None or pd.isna(phone):
        return None
    return "".join(c for c in str(phone).split(".")[0] if c.isdigit())


@task
def extract_data(source_db):
    logger = get_run_logger()  # ← Logging requirement!
    logger.info(f"Extracting from {source_db}")

    conn = sqlite3.connect(source_db)
    customers = pd.read_sql("SELECT * FROM vw_raw_customers", conn)
    orders = pd.read_sql("SELECT * FROM vw_raw_orders", conn)
    rates = pd.read_sql("SELECT * FROM vw_exchange_rates", conn)
    conn.close()

    logger.info(
        f"Extracted: {len(customers)} customers, {len(orders)} orders, {len(rates)} rates"
    )
    return customers, orders, rates


@task(retries=2, retry_delay_seconds=5)  # ← เพิ่ม retry ถ้า network fail
def clean_customers(df):
    logger = get_run_logger()
    logger.info(f"Cleaning {len(df)} customer records")

    # dedupe
    df = df.sort_values("signup_date").drop_duplicates("customer_id", keep="last")
    # phone
    df["phone"] = df["phone"].apply(clean_phone)
    # email
    df["email"] = df["email"].fillna("unknown@domain.com")

    logger.info(f"After cleaning: {len(df)} unique customers")
    return df


def _clean_orders_logic(orders_df, rates_df):
    df = orders_df[orders_df["total_amount"] > 0].copy()
    df["currency"] = df["currency"].fillna("USD")
    df = df.merge(
        rates_df,
        left_on=["currency", "order_date"],
        right_on=["currency", "date"],
        how="left",
    )
    df["rate_to_usd"] = df["rate_to_usd"].fillna(1.0)
    df["usd_amount"] = df["total_amount"] * df["rate_to_usd"]
    return df


@task
def clean_orders(orders_df, rates_df):
    logger = get_run_logger()
    logger.info(f"Cleaning {len(orders_df)} orders")
    result = _clean_orders_logic(orders_df, rates_df)
    logger.info(f"After cleaning: {len(result)} valid records")
    return result


@task
def load_data(customers, orders, target_db):
    logger = get_run_logger()

    output_dir = Path(target_db).parent
    output_dir.mkdir(exist_ok=True)

    conn = sqlite3.connect(target_db)
    customers.to_sql("dim_customers", conn, if_exists="replace", index=False)
    orders.to_sql("fct_orders", conn, if_exists="replace", index=False)
    conn.close()

    logger.info(f"Loaded to {target_db}")


@flow(name="shopdata-etl")
def etl_pipeline(
    source_db: str = "data\shopdata_01.db",
    target_db: str = "output\analytics.db",
):
    logger = get_run_logger()
    logger.info("🚀 Starting ShopData ETL Pipeline")

    try:
        # Extract
        customers_raw, orders_raw, rates = extract_data(source_db)

        # Transform
        customers_clean = clean_customers(customers_raw)
        orders_clean = clean_orders(orders_raw, rates)

        # Load
        load_data(customers_clean, orders_clean, target_db)

        logger.info("✅ Pipeline completed successfully")
    except Exception as e:
        logger.error(f"❌ Pipeline failed: {type(e).__name__}: {e}")
        raise  # re-raise ให้ Prefect รู้ว่า fail


# ─── Entry point ──────────────────────────
if __name__ == "__main__":
    etl_pipeline(
        source_db=r"C:\Users\User\Desktop\storemesh-de-test\data\shopdata_01.db",
        target_db=r"C:\Users\User\Desktop\storemesh-de-test\output\analytics.db",
    )
