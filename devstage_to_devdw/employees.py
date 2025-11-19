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

def load_employees_to_devdw():
    """Perform update and insert from devstage → devdw employees."""
    etl_batch_no, etl_batch_date = get_batch_date_from_redshift()
    conn = get_redshift_connection()
    cur = conn.cursor()

    print(" Running UPDATE on existing employees...")
    update_query = f"""
        UPDATE j25renu_devdw.employees AS d
        SET
            lastName = s.lastName,
            firstName = s.firstName,
            extension = s.extension,
            email = s.email,
            officeCode = s.officeCode,
            reportsTo = s.reportsTo,
            jobTitle = s.jobTitle,
            src_update_timestamp = s.update_timestamp,
            dw_update_timestamp = CURRENT_TIMESTAMP,
            etl_batch_no = {etl_batch_no},
            etl_batch_date = '{etl_batch_date}'
        FROM j25renu_devstage.employees AS s
        WHERE d.employeeNumber = s.employeeNumber;
    """
    cur.execute(update_query)
    print(" Updated existing records.")

    print(" Running INSERT for new employees...")
    insert_query = f"""
        INSERT INTO j25renu_devdw.employees (
            employeeNumber,
            lastName,
            firstName,
            extension,
            email,
            officeCode,
            reportsTo,
            jobTitle,
            dw_office_id,
            dw_reporting_employee_id,
            src_create_timestamp,
            src_update_timestamp,
            dw_create_timestamp,
            dw_update_timestamp,
            etl_batch_no,
            etl_batch_date
        )
        SELECT
            s.employeeNumber,
            s.lastName,
            s.firstName,
            s.extension,
            s.email,
            s.officeCode,
            s.reportsTo,
            s.jobTitle,
            o.dw_office_id,
            NULL AS dw_reporting_employee_id,
            s.create_timestamp,
            s.update_timestamp,
            CURRENT_TIMESTAMP,
            CURRENT_TIMESTAMP,
            {etl_batch_no},
            '{etl_batch_date}'
        FROM j25renu_devstage.employees AS s
        LEFT JOIN j25renu_devdw.offices AS o
            ON s.officeCode = o.officeCode
        LEFT JOIN j25renu_devdw.employees AS d
            ON s.employeeNumber = d.employeeNumber
        WHERE d.employeeNumber IS NULL;
    """
    cur.execute(insert_query)
    print(" Inserted new records.")

    print(" Updating reporting hierarchy...")
    hierarchy_update = f"""
        UPDATE j25renu_devdw.employees AS e
        SET dw_reporting_employee_id = r.dw_employee_id
        FROM j25renu_devdw.employees AS r
        WHERE e.reportsTo = r.employeeNumber
          AND r.src_create_timestamp >= DATE '{etl_batch_date}';
    """
    cur.execute(hierarchy_update)
    print(" Updated reporting hierarchy.")

    cur.close()
    conn.close()
    print(" Redshift connection closed.")


if __name__ == "__main__":
    load_employees_to_devdw()
