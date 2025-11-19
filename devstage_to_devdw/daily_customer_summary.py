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

def load_daily_customer_summary():
    """Generate daily customer summary from DW tables and insert into devdw.daily_customer_summary."""
    etl_batch_no, etl_batch_date = get_batch_date_from_redshift()
    conn = get_redshift_connection()
    cur = conn.cursor()

    print(" Inserting data into daily_customer_summary...")

    insert_query = f"""
        INSERT INTO j25renu_devdw.daily_customer_summary (
            summary_date,
            dw_customer_id,
            order_count,
            order_apd,
            order_cost_amount,
            cancelled_order_count,
            cancelled_order_amount,
            cancelled_order_apd,
            shipped_order_count,
            shipped_order_amount,
            shipped_order_apd,
            payment_apd,
            payment_amount,
            products_ordered_qty,
            products_items_qty,
            order_mrp_amount,
            new_customer_apd,
            new_customer_paid_apd,
            dw_create_timestamp,
            dw_update_timestamp,
            etl_batch_no,
            etl_batch_date
        )
        WITH orders_cte AS (
            SELECT 
                CAST(o.orderDate AS DATE) AS summary_date,
                o.dw_customer_id,
                COUNT(DISTINCT o.dw_order_id) AS order_count,
                1 AS order_apd,
                SUM(od.priceEach * od.quantityOrdered) AS order_cost_amount,
                0 AS cancelled_order_count,
                0 AS cancelled_order_amount,
                0 AS cancelled_order_apd,
                0 AS shipped_order_count,
                0 AS shipped_order_amount,
                0 AS shipped_order_apd,
                0 AS payment_apd,
                0 AS payment_amount,
                COUNT(DISTINCT od.dw_product_id) AS products_ordered_qty,
                COUNT(od.quantityOrdered) AS products_items_qty,
                SUM(p.MSRP * od.quantityOrdered) AS order_mrp_amount,
                0 AS new_customer_apd,
                0 AS new_customer_paid_apd
            FROM j25renu_devdw.orders o
            JOIN j25renu_devdw.orderdetails od ON o.dw_order_id = od.dw_order_id
            JOIN j25renu_devdw.products p ON od.dw_product_id = p.dw_product_id
            WHERE CAST(o.orderDate AS DATE) >= DATE '{etl_batch_date}'
            GROUP BY 1,2
        ),
        customers_cte AS (
            SELECT 
                CAST(c.src_create_timestamp AS DATE) AS summary_date,
                c.dw_customer_id,
                0 AS order_count,
                0 AS order_apd,
                0 AS order_cost_amount,
                0 AS cancelled_order_count,
                0 AS cancelled_order_amount,
                0 AS cancelled_order_apd,
                0 AS shipped_order_count,
                0 AS shipped_order_amount,
                0 AS shipped_order_apd,
                0 AS payment_apd,
                0 AS payment_amount,
                0 AS products_ordered_qty,
                0 AS products_items_qty,
                0 AS order_mrp_amount,
                1 AS new_customer_apd,
                0 AS new_customer_paid_apd
            FROM j25renu_devdw.customers c
            WHERE CAST(c.src_create_timestamp AS DATE) >= DATE '{etl_batch_date}'
        ),
        cancelled_cte AS (
            SELECT 
                CAST(o.cancelledDate AS DATE) AS summary_date,
                o.dw_customer_id,
                0 AS order_count,
                0 AS order_apd,
                0 AS order_cost_amount,
                COUNT(o.dw_order_id) AS cancelled_order_count,
                SUM(od.priceEach * od.quantityOrdered) AS cancelled_order_amount,
                1 AS cancelled_order_apd,
                0 AS shipped_order_count,
                0 AS shipped_order_amount,
                0 AS shipped_order_apd,
                0 AS payment_apd,
                0 AS payment_amount,
                0 AS products_ordered_qty,
                0 AS products_items_qty,
                0 AS order_mrp_amount,
                0 AS new_customer_apd,
                0 AS new_customer_paid_apd
            FROM j25renu_devdw.orders o
            JOIN j25renu_devdw.orderdetails od ON o.dw_order_id = od.dw_order_id
            WHERE CAST(o.cancelledDate AS DATE) >= DATE '{etl_batch_date}'
              AND o.status = 'Cancelled'
            GROUP BY 1,2
        ),
        payments_cte AS (
            SELECT 
                CAST(p.paymentDate AS DATE) AS summary_date,
                p.dw_customer_id,
                0 AS order_count,
                0 AS order_apd,
                0 AS order_cost_amount,
                0 AS cancelled_order_count,
                0 AS cancelled_order_amount,
                0 AS cancelled_order_apd,
                0 AS shipped_order_count,
                0 AS shipped_order_amount,
                0 AS shipped_order_apd,
                1 AS payment_apd,
                SUM(p.amount) AS payment_amount,
                0 AS products_ordered_qty,
                0 AS products_items_qty,
                0 AS order_mrp_amount,
                0 AS new_customer_apd,
                1 AS new_customer_paid_apd
            FROM j25renu_devdw.payments p
            WHERE CAST(p.paymentDate AS DATE) >= DATE '{etl_batch_date}'
            GROUP BY 1,2
        ),
        shipped_cte AS (
            SELECT 
                CAST(o.shippedDate AS DATE) AS summary_date,
                o.dw_customer_id,
                0 AS order_count,
                0 AS order_apd,
                0 AS order_cost_amount,
                0 AS cancelled_order_count,
                0 AS cancelled_order_amount,
                0 AS cancelled_order_apd,
                COUNT(o.dw_order_id) AS shipped_order_count,
                SUM(od.priceEach * od.quantityOrdered) AS shipped_order_amount,
                1 AS shipped_order_apd,
                0 AS payment_apd,
                0 AS payment_amount,
                0 AS products_ordered_qty,
                0 AS products_items_qty,
                0 AS order_mrp_amount,
                0 AS new_customer_apd,
                0 AS new_customer_paid_apd
            FROM j25renu_devdw.orders o
            JOIN j25renu_devdw.orderdetails od ON o.dw_order_id = od.dw_order_id
            WHERE CAST(o.shippedDate AS DATE) >= DATE '{etl_batch_date}'
              AND o.status = 'Shipped'
            GROUP BY 1,2
        ),
        combined_cte AS (
            SELECT * FROM orders_cte
            UNION ALL
            SELECT * FROM customers_cte
            UNION ALL
            SELECT * FROM cancelled_cte
            UNION ALL
            SELECT * FROM payments_cte
            UNION ALL
            SELECT * FROM shipped_cte
        )
        SELECT
            summary_date,
            dw_customer_id,
            MAX(order_count),
            MAX(order_apd),
            MAX(order_cost_amount),
            MAX(cancelled_order_count),
            MAX(cancelled_order_amount),
            MAX(cancelled_order_apd),
            MAX(shipped_order_count),
            MAX(shipped_order_amount),
            MAX(shipped_order_apd),
            MAX(payment_apd),
            MAX(payment_amount),
            MAX(products_ordered_qty),
            MAX(products_items_qty),
            MAX(order_mrp_amount),
            MAX(new_customer_apd),
            MAX(new_customer_paid_apd),
            CURRENT_TIMESTAMP,
            CURRENT_TIMESTAMP,
            {etl_batch_no},
            '{etl_batch_date}'
        FROM combined_cte
        GROUP BY summary_date, dw_customer_id;
    """

    cur.execute(insert_query)
    print(" Inserted new records into daily_customer_summary.")

    cur.close()
    conn.close()
    print(" Redshift connection closed.")


if __name__ == "__main__":
    load_daily_customer_summary()
