import psycopg2
import os
from dotenv import load_dotenv
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db_utils import get_batch_date_from_redshift
# Load environment variables
load_dotenv()
REDSHIFT_HOST = os.getenv("REDSHIFT_HOST")
REDSHIFT_PORT = os.getenv("REDSHIFT_PORT")
REDSHIFT_DB = os.getenv("REDSHIFT_DB")
REDSHIFT_USER = os.getenv("REDSHIFT_USER")
REDSHIFT_PASSWORD = os.getenv("REDSHIFT_PASSWORD")
REDSHIFT_SCHEMA = 'j25renu_etl_metadata'
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

def load_customers_to_devdw():
    """Perform update and insert from devstage → devdw customers."""
    etl_batch_no, etl_batch_date = get_batch_date_from_redshift()
    conn = get_redshift_connection()
    cur = conn.cursor()

    print(" Running UPDATE on existing customers...")
    update_query = f"""
        UPDATE j25renu_devdw.customers AS d
        SET
            customerName = s.customerName,
            contactLastName = s.contactLastName,
            contactFirstName = s.contactFirstName,
            phone = s.phone,
            addressLine1 = s.addressLine1,
            addressLine2 = s.addressLine2,
            city = s.city,
            state = s.state,
            postalCode = s.postalCode,
            country = s.country,
            salesRepEmployeeNumber = s.salesRepEmployeeNumber,
            creditLimit = s.creditLimit,
            src_update_timestamp = s.update_timestamp,
            dw_update_timestamp = CURRENT_TIMESTAMP,
            etl_batch_no = {etl_batch_no},
            etl_batch_date = '{etl_batch_date}'
        FROM j25renu_devstage.customers AS s
        WHERE d.src_customerNumber = s.customerNumber;
    """

    cur.execute(update_query)
    print("Updated existing records.")

    print(" Running INSERT for new customers...")
    insert_query = f"""
        INSERT INTO j25renu_devdw.customers (
            src_customerNumber,
            customerName,
            contactLastName,
            contactFirstName,
            phone,
            addressLine1,
            addressLine2,
            city,
            state,
            postalCode,
            country,
            salesRepEmployeeNumber,
            creditLimit,
            src_create_timestamp,
            src_update_timestamp,
            dw_create_timestamp,
            dw_update_timestamp,
            etl_batch_no,
            etl_batch_date
        )
        SELECT
            s.customerNumber,
            s.customerName,
            s.contactLastName,
            s.contactFirstName,
            s.phone,
            s.addressLine1,
            s.addressLine2,
            s.city,
            s.state,
            s.postalCode,
            s.country,
            s.salesRepEmployeeNumber,
            s.creditLimit,
            s.create_timestamp,
            s.update_timestamp,
            CURRENT_TIMESTAMP,
            CURRENT_TIMESTAMP,
            {etl_batch_no},
            '{etl_batch_date}'
        FROM j25renu_devstage.customers AS s
        LEFT JOIN j25renu_devdw.customers AS d
          ON s.customerNumber = d.src_customerNumber
        WHERE d.src_customerNumber IS NULL;
    """

    cur.execute(insert_query)
    print(" Inserted new records.")

    cur.close()
    conn.close()
    print(" Redshift connection closed.")


if __name__ == "__main__":
    load_customers_to_devdw()
