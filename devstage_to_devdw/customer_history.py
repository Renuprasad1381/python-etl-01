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


def load_customer_history():
    """Perform SCD Type-2 update and insert for customer credit limit history."""
    etl_batch_no, etl_batch_date = get_batch_date_from_redshift()
    conn = get_redshift_connection()
    cur = conn.cursor()

    # Step 1: Update existing history records where credit limit changed
    print(" Running UPDATE on existing customer_history records...")
    update_query = f"""
        UPDATE j25renu_devdw.customer_history AS h
        SET 
            effective_to_date = DATEADD(day, -1, DATE '{etl_batch_date}'),
            dw_active_record_ind = 0,
            dw_update_timestamp = CURRENT_TIMESTAMP,
            update_etl_batch_no = {etl_batch_no},
            update_etl_batch_date = '{etl_batch_date}'
        FROM j25renu_devdw.customers AS c
        WHERE h.dw_customer_id = c.dw_customer_id
          AND h.dw_active_record_ind = 1
          AND h.creditLimit <> c.creditLimit;
    """
    cur.execute(update_query)
    print(" Updated inactive records in customer_history.")

    # Step 2: Insert new active records
    print(" Running INSERT for new/changed customer_history records...")
    insert_query = f"""
        INSERT INTO j25renu_devdw.customer_history (
            dw_customer_id,
            creditLimit,
            effective_from_date,
            effective_to_date,
            dw_active_record_ind,
            dw_create_timestamp,
            dw_update_timestamp,
            create_etl_batch_no,
            create_etl_batch_date,
            update_etl_batch_no,
            update_etl_batch_date
        )
        SELECT 
            c.dw_customer_id,
            c.creditLimit,
            DATE '{etl_batch_date}' AS effective_from_date,
            NULL AS effective_to_date,
            1 AS dw_active_record_ind,
            CURRENT_TIMESTAMP AS dw_create_timestamp,
            CURRENT_TIMESTAMP AS dw_update_timestamp,
            {etl_batch_no} AS create_etl_batch_no,
            '{etl_batch_date}' AS create_etl_batch_date,
            NULL AS update_etl_batch_no,
            NULL AS update_etl_batch_date
        FROM j25renu_devdw.customers AS c
        LEFT JOIN j25renu_devdw.customer_history AS h
          ON c.dw_customer_id = h.dw_customer_id
          AND h.dw_active_record_ind = 1
        WHERE h.dw_customer_id IS NULL
           OR h.creditLimit <> c.creditLimit;
    """
    cur.execute(insert_query)
    print(" Inserted new active customer_history records.")

    cur.close()
    conn.close()
    print(" Redshift connection closed.")


if __name__ == "__main__":
    load_customer_history()
