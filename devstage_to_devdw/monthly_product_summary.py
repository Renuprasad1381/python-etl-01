import psycopg2
import os
from dotenv import load_dotenv
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db_utils import get_batch_date_from_redshift

# Load environment variables
load_dotenv()

# ========== REDSHIFT CONNECTION CONFIG ==========
REDSHIFT_HOST = os.getenv("REDSHIFT_HOST")
REDSHIFT_PORT = os.getenv("REDSHIFT_PORT")
REDSHIFT_DB = os.getenv("REDSHIFT_DB")
REDSHIFT_USER = os.getenv("REDSHIFT_USER")
REDSHIFT_PASSWORD = os.getenv("REDSHIFT_PASSWORD")
# =================================================


def get_redshift_connection():
    """Create and return a Redshift connection."""
    conn = psycopg2.connect(
        host=REDSHIFT_HOST,
        port=REDSHIFT_PORT,
        dbname=REDSHIFT_DB,
        user=REDSHIFT_USER,
        password=REDSHIFT_PASSWORD
    )
    conn.autocommit = True
    return conn


def load_monthly_product_summary():
    """Aggregate daily_product_summary data into monthly_product_summary table."""
    etl_batch_no, etl_batch_date = get_batch_date_from_redshift()
    conn = get_redshift_connection()
    cur = conn.cursor()

    print(" Running monthly aggregation and merge into monthly_product_summary...")

    sql = f"""
        -- ========================
        -- Step 1: UPDATE existing records
        -- ========================
        UPDATE j25renu_devdw.monthly_product_summary AS m
        SET
            customer_apd = m.customer_apd + d.customer_apd,
            product_cost_amount = m.product_cost_amount + d.product_cost_amount,
            product_mrp_amount = m.product_mrp_amount + d.product_mrp_amount,
            cancelled_product_qty = m.cancelled_product_qty + d.cancelled_product_qty,
            cancelled_cost_amount = m.cancelled_cost_amount + d.cancelled_cost_amount,
            cancelled_mrp_amount = m.cancelled_mrp_amount + d.cancelled_mrp_amount,
            cancelled_order_apd = m.cancelled_order_apd + d.cancelled_order_apd,
            cancelled_order_apm = m.cancelled_order_apm + d.cancelled_order_apm,
            dw_update_timestamp = CURRENT_TIMESTAMP,
            etl_batch_no = d.etl_batch_no,
            etl_batch_date = d.etl_batch_date
        FROM (
            SELECT
                DATE_TRUNC('month', summary_date)::DATE AS start_of_the_month_date,
                dw_product_id,
                MAX(customer_apd) AS customer_apd,
                1 AS customer_apm,
                SUM(product_cost_amount) AS product_cost_amount,
                SUM(product_mrp_amount) AS product_mrp_amount,
                SUM(cancelled_product_qty) AS cancelled_product_qty,
                SUM(cancelled_cost_amount) AS cancelled_cost_amount,
                SUM(cancelled_mrp_amount) AS cancelled_mrp_amount,
                MAX(cancelled_order_apd) AS cancelled_order_apd,
                SUM(cancelled_order_apd) AS cancelled_order_apm,
                MAX(dw_create_timestamp) AS dw_create_timestamp,
                MAX(dw_update_timestamp) AS dw_update_timestamp,
                MAX(etl_batch_no) AS etl_batch_no,
                MAX(etl_batch_date) AS etl_batch_date
            FROM j25renu_devdw.daily_product_summary
            WHERE etl_batch_date >= DATE '{etl_batch_date}'
            GROUP BY 1, 2
        ) AS d
        WHERE m.start_of_the_month_date = d.start_of_the_month_date
          AND m.dw_product_id = d.dw_product_id;

        -- ========================
        -- Step 2: INSERT new records
        -- ========================
        INSERT INTO j25renu_devdw.monthly_product_summary (
            start_of_the_month_date,
            dw_product_id,
            customer_apd,
            customer_apm,
            product_cost_amount,
            product_mrp_amount,
            cancelled_product_qty,
            cancelled_cost_amount,
            cancelled_mrp_amount,
            cancelled_order_apd,
            cancelled_order_apm,
            dw_create_timestamp,
            dw_update_timestamp,
            etl_batch_no,
            etl_batch_date
        )
        SELECT
            d.start_of_the_month_date,
            d.dw_product_id,
            d.customer_apd,
            d.customer_apm,
            d.product_cost_amount,
            d.product_mrp_amount,
            d.cancelled_product_qty,
            d.cancelled_cost_amount,
            d.cancelled_mrp_amount,
            d.cancelled_order_apd,
            d.cancelled_order_apm,
            CURRENT_TIMESTAMP,
            CURRENT_TIMESTAMP,
            d.etl_batch_no,
            d.etl_batch_date
        FROM (
            SELECT
                DATE_TRUNC('month', summary_date)::DATE AS start_of_the_month_date,
                dw_product_id,
                MAX(customer_apd) AS customer_apd,
                1 AS customer_apm,
                SUM(product_cost_amount) AS product_cost_amount,
                SUM(product_mrp_amount) AS product_mrp_amount,
                SUM(cancelled_product_qty) AS cancelled_product_qty,
                SUM(cancelled_cost_amount) AS cancelled_cost_amount,
                SUM(cancelled_mrp_amount) AS cancelled_mrp_amount,
                MAX(cancelled_order_apd) AS cancelled_order_apd,
                SUM(cancelled_order_apd) AS cancelled_order_apm,
                MAX(dw_create_timestamp) AS dw_create_timestamp,
                MAX(dw_update_timestamp) AS dw_update_timestamp,
                MAX(etl_batch_no) AS etl_batch_no,
                MAX(etl_batch_date) AS etl_batch_date
            FROM j25renu_devdw.daily_product_summary
            WHERE etl_batch_date >= DATE '{etl_batch_date}'
            GROUP BY 1, 2
        ) AS d
        LEFT JOIN j25renu_devdw.monthly_product_summary AS m
          ON m.start_of_the_month_date = d.start_of_the_month_date
         AND m.dw_product_id = d.dw_product_id
        WHERE m.dw_product_id IS NULL;
    """

    cur.execute(sql)
    print(" Monthly product summary successfully updated and inserted.")

    cur.close()
    conn.close()
    print(" Redshift connection closed.")


if __name__ == "__main__":
    load_monthly_product_summary()
