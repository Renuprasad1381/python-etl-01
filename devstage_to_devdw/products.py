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


def load_products_to_devdw():
    """Perform update and insert from devstage → devdw products."""
    etl_batch_no, etl_batch_date = get_batch_date_from_redshift()
    conn = get_redshift_connection()
    cur = conn.cursor()

    print(" Running UPDATE on existing products...")
    update_query = f"""
        UPDATE j25renu_devdw.products AS d
        SET
            productName = s.productName,
            productLine = s.productLine,
            productScale = s.productScale,
            productVendor = s.productVendor,
            productDescription = s.productDescription,
            quantityInStock = s.quantityInStock,
            buyPrice = s.buyPrice,
            MSRP = s.MSRP,
            dw_product_line_id = pl.dw_product_line_id,
            src_update_timestamp = s.update_timestamp,
            dw_update_timestamp = CURRENT_TIMESTAMP,
            etl_batch_no = {etl_batch_no},
            etl_batch_date = '{etl_batch_date}'
        FROM j25renu_devstage.products AS s
        LEFT JOIN j25renu_devdw.productlines AS pl
            ON s.productLine = pl.productLine
        WHERE d.src_productCode = s.productCode
          AND s.update_timestamp >= DATE '{etl_batch_date}';
    """
    cur.execute(update_query)
    print(" Updated existing products.")

    print(" Running INSERT for new products...")
    insert_query = f"""
        INSERT INTO j25renu_devdw.products (
            src_productCode,
            productName,
            productLine,
            productScale,
            productVendor,
            productDescription,
            quantityInStock,
            buyPrice,
            MSRP,
            dw_product_line_id,
            src_create_timestamp,
            src_update_timestamp,
            dw_create_timestamp,
            dw_update_timestamp,
            etl_batch_no,
            etl_batch_date
        )
        SELECT
            s.productCode,
            s.productName,
            s.productLine,
            s.productScale,
            s.productVendor,
            s.productDescription,
            s.quantityInStock,
            s.buyPrice,
            s.MSRP,
            pl.dw_product_line_id,
            s.create_timestamp,
            s.update_timestamp,
            CURRENT_TIMESTAMP,
            CURRENT_TIMESTAMP,
            {etl_batch_no},
            '{etl_batch_date}'
        FROM j25renu_devstage.products AS s
        LEFT JOIN j25renu_devdw.products AS d
            ON s.productCode = d.src_productCode
        LEFT JOIN j25renu_devdw.productlines AS pl
            ON s.productLine = pl.productLine
        WHERE d.src_productCode IS NULL
          AND s.create_timestamp >= DATE '{etl_batch_date}';
    """
    cur.execute(insert_query)
    print(" Inserted new products.")

    cur.close()
    conn.close()
    print(" Redshift connection closed.")


if __name__ == "__main__":
    load_products_to_devdw()
