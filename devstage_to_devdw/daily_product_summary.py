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


def load_daily_product_summary():
    """Generate and insert daily product summary data into devdw.daily_product_summary."""
    etl_batch_no, etl_batch_date = get_batch_date_from_redshift()
    conn = get_redshift_connection()
    cur = conn.cursor()

    print(" Inserting data into daily_product_summary...")

    insert_query = f"""
        INSERT INTO j25renu_devdw.daily_product_summary (
            summary_date,
            dw_product_id,
            customer_apd,
            product_cost_amount,
            product_mrp_amount,
            cancelled_product_qty,
            cancelled_cost_amount,
            cancelled_mrp_amount,
            cancelled_order_apd,
            dw_create_timestamp,
            dw_update_timestamp,
            etl_batch_no,
            etl_batch_date
        )
        WITH product_sales_cte AS (
            SELECT
                CAST(o.orderDate AS DATE) AS summary_date,
                od.dw_product_id,
                COUNT(DISTINCT o.dw_customer_id) AS customer_apd,
                SUM(od.priceEach * od.quantityOrdered) AS product_cost_amount,
                SUM(p.MSRP * od.quantityOrdered) AS product_mrp_amount,
                0 AS cancelled_product_qty,
                0 AS cancelled_cost_amount,
                0 AS cancelled_mrp_amount,
                0 AS cancelled_order_apd
            FROM j25renu_devdw.orders o
            JOIN j25renu_devdw.orderdetails od ON o.dw_order_id = od.dw_order_id
            JOIN j25renu_devdw.products p ON od.dw_product_id = p.dw_product_id
            WHERE CAST(o.orderDate AS DATE) >= DATE '{etl_batch_date}'
            GROUP BY summary_date, od.dw_product_id
        ),
        cancelled_products_cte AS (
            SELECT
                CAST(o.cancelledDate AS DATE) AS summary_date,
                od.dw_product_id,
                0 AS customer_apd,
                0 AS product_cost_amount,
                0 AS product_mrp_amount,
                SUM(od.quantityOrdered) AS cancelled_product_qty,
                SUM(od.priceEach * od.quantityOrdered) AS cancelled_cost_amount,
                SUM(p.MSRP * od.quantityOrdered) AS cancelled_mrp_amount,
                COUNT(DISTINCT o.dw_order_id) AS cancelled_order_apd
            FROM j25renu_devdw.orders o
            JOIN j25renu_devdw.orderdetails od ON o.dw_order_id = od.dw_order_id
            JOIN j25renu_devdw.products p ON od.dw_product_id = p.dw_product_id
            WHERE LOWER(TRIM(o.status)) = 'cancelled'
              AND CAST(o.cancelledDate AS DATE) >= DATE '{etl_batch_date}'
            GROUP BY summary_date, od.dw_product_id
        ),
        combined_cte AS (
            SELECT * FROM product_sales_cte
            UNION ALL
            SELECT * FROM cancelled_products_cte
        )
        SELECT
            summary_date,
            dw_product_id,
            MAX(customer_apd) AS customer_apd,
            MAX(product_cost_amount) AS product_cost_amount,
            MAX(product_mrp_amount) AS product_mrp_amount,
            MAX(cancelled_product_qty) AS cancelled_product_qty,
            MAX(cancelled_cost_amount) AS cancelled_cost_amount,
            MAX(cancelled_mrp_amount) AS cancelled_mrp_amount,
            MAX(cancelled_order_apd) AS cancelled_order_apd,
            CURRENT_TIMESTAMP AS dw_create_timestamp,
            CURRENT_TIMESTAMP AS dw_update_timestamp,
            {etl_batch_no} AS etl_batch_no,
            '{etl_batch_date}' AS etl_batch_date
        FROM combined_cte
        GROUP BY summary_date, dw_product_id;
    """

    cur.execute(insert_query)
    print(" Inserted new records into daily_product_summary.")

    cur.close()
    conn.close()
    print(" Redshift connection closed.")


if __name__ == "__main__":
    load_daily_product_summary()
