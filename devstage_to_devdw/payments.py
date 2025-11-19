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


def load_payments_to_devdw():
    """Perform update and insert from devstage → devdw payments."""
    etl_batch_no, etl_batch_date = get_batch_date_from_redshift()
    conn = get_redshift_connection()
    cur = conn.cursor()

    print(" Running UPDATE on existing payments...")
    update_query = f"""
        UPDATE j25renu_devdw.payments AS d
        SET
            paymentDate = s.paymentDate,
            amount = s.amount,
            dw_customer_id = c.dw_customer_id,
            src_update_timestamp = s.update_timestamp,
            dw_update_timestamp = CURRENT_TIMESTAMP,
            etl_batch_no = {etl_batch_no},
            etl_batch_date = '{etl_batch_date}'
        FROM j25renu_devstage.payments AS s
        LEFT JOIN j25renu_devdw.customers AS c
            ON s.customerNumber = c.src_customerNumber
        WHERE d.src_customerNumber = s.customerNumber
          AND d.checkNumber = s.checkNumber;
    """
    cur.execute(update_query)
    print(" Updated existing payment records.")

    print(" Running INSERT for new payments...")
    insert_query = f"""
        INSERT INTO j25renu_devdw.payments (
            dw_customer_id,
            src_customerNumber,
            checkNumber,
            paymentDate,
            amount,
            src_create_timestamp,
            src_update_timestamp,
            dw_create_timestamp,
            dw_update_timestamp,
            etl_batch_no,
            etl_batch_date
        )
        SELECT
            c.dw_customer_id,
            s.customerNumber,
            s.checkNumber,
            s.paymentDate,
            s.amount,
            s.create_timestamp,
            s.update_timestamp,
            CURRENT_TIMESTAMP,
            CURRENT_TIMESTAMP,
            {etl_batch_no},
            '{etl_batch_date}'
        FROM j25renu_devstage.payments AS s
        LEFT JOIN j25renu_devdw.payments AS d
            ON s.customerNumber = d.src_customerNumber
           AND s.checkNumber = d.checkNumber
        LEFT JOIN j25renu_devdw.customers AS c
            ON s.customerNumber = c.src_customerNumber
        WHERE d.src_customerNumber IS NULL;
    """
    cur.execute(insert_query)
    print(" Inserted new payments.")

    cur.close()
    conn.close()
    print(" Redshift connection closed.")


if __name__ == "__main__":
    load_payments_to_devdw()
