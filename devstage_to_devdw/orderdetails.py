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

def load_orderdetails_to_devdw():
    """Perform update and insert from devstage → devdw orderdetails."""
    etl_batch_no, etl_batch_date = get_batch_date_from_redshift()
    conn = get_redshift_connection()
    cur = conn.cursor()

    print(" Running UPDATE on existing orderdetails...")
    update_query = f"""
        UPDATE j25renu_devdw.orderdetails AS d
        SET
            quantityOrdered = s.quantityOrdered,
            priceEach = s.priceEach,
            src_update_timestamp = s.update_timestamp,
            dw_update_timestamp = CURRENT_TIMESTAMP,
            etl_batch_no = {etl_batch_no},
            etl_batch_date = '{etl_batch_date}'
        FROM j25renu_devstage.orderdetails AS s
        WHERE d.src_orderNumber = s.orderNumber
          AND d.src_productCode = s.productCode;
    """
    cur.execute(update_query)
    print(" Updated existing orderdetails.")

    print(" Running INSERT for new orderdetails...")
    insert_query = f"""
        INSERT INTO j25renu_devdw.orderdetails (
            dw_order_id,
            dw_product_id,
            src_orderNumber,
            src_productCode,
            quantityOrdered,
            priceEach,
            orderLineNumber,
            src_create_timestamp,
            src_update_timestamp,
            dw_create_timestamp,
            dw_update_timestamp,
            etl_batch_no,
            etl_batch_date
        )
        SELECT
            o.dw_order_id,
            p.dw_product_id,
            s.orderNumber,
            s.productCode,
            s.quantityOrdered,
            s.priceEach,
            s.orderLineNumber,
            s.create_timestamp,
            s.update_timestamp,
            CURRENT_TIMESTAMP,
            CURRENT_TIMESTAMP,
            {etl_batch_no},
            '{etl_batch_date}'
        FROM j25renu_devstage.orderdetails AS s
        LEFT JOIN j25renu_devdw.orderdetails AS d
            ON s.orderNumber = d.src_orderNumber
           AND s.productCode = d.src_productCode
        LEFT JOIN j25renu_devdw.orders AS o
            ON o.src_orderNumber = s.orderNumber
        LEFT JOIN j25renu_devdw.products AS p
            ON p.src_productCode = s.productCode
        WHERE d.src_orderNumber IS NULL;
    """
    cur.execute(insert_query)
    print(" Inserted new orderdetails.")

    cur.close()
    conn.close()
    print(" Redshift connection closed.")


if __name__ == "__main__":
    load_orderdetails_to_devdw()
