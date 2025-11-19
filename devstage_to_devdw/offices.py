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


def load_offices_to_devdw():
    """Perform update and insert from devstage → devdw offices."""
    etl_batch_no, etl_batch_date = get_batch_date_from_redshift()
    conn = get_redshift_connection()
    cur = conn.cursor()

    print(" Running UPDATE on existing offices...")
    update_query = f"""
        UPDATE j25renu_devdw.offices AS d
        SET
            city = s.city,
            phone = s.phone,
            addressLine1 = s.addressLine1,
            addressLine2 = s.addressLine2,
            state = s.state,
            country = s.country,
            postalCode = s.postalCode,
            territory = s.territory,
            src_update_timestamp = s.update_timestamp,
            dw_update_timestamp = CURRENT_TIMESTAMP,
            etl_batch_no = {etl_batch_no},
            etl_batch_date = '{etl_batch_date}'
        FROM j25renu_devstage.offices AS s
        WHERE d.officeCode = s.officeCode;
    """

    cur.execute(update_query)
    print(" Updated existing records.")

    print(" Running INSERT for new offices...")
    insert_query = f"""
        INSERT INTO j25renu_devdw.offices (
            officeCode,
            city,
            phone,
            addressLine1,
            addressLine2,
            state,
            country,
            postalCode,
            territory,
            src_create_timestamp,
            src_update_timestamp,
            dw_create_timestamp,
            dw_update_timestamp,
            etl_batch_no,
            etl_batch_date
        )
        SELECT
            s.officeCode,
            s.city,
            s.phone,
            s.addressLine1,
            s.addressLine2,
            s.state,
            s.country,
            s.postalCode,
            s.territory,
            s.create_timestamp,
            s.update_timestamp,
            CURRENT_TIMESTAMP,
            CURRENT_TIMESTAMP,
            {etl_batch_no},
            '{etl_batch_date}'
        FROM j25renu_devstage.offices AS s
        LEFT JOIN j25renu_devdw.offices AS d
          ON s.officeCode = d.officeCode
        WHERE d.officeCode IS NULL;
    """

    cur.execute(insert_query)
    print(" Inserted new records.")

    cur.close()
    conn.close()
    print(" Redshift connection closed.")


if __name__ == "__main__":
    load_offices_to_devdw()
