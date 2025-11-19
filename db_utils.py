import os
import oracledb
from dotenv import load_dotenv
import psycopg2

# Load environment variables
load_dotenv()

user = os.getenv("ORACLE_USER")
password = os.getenv("ORACLE_PASSWORD")
host = os.getenv("ORACLE_HOST")
port = os.getenv("ORACLE_PORT")        
service = os.getenv("ORACLE_SERVICE")

REDSHIFT_HOST = os.getenv("REDSHIFT_HOST")
REDSHIFT_PORT = os.getenv("REDSHIFT_PORT")
REDSHIFT_DB = os.getenv("REDSHIFT_DB")
REDSHIFT_USER = os.getenv("REDSHIFT_USER")
REDSHIFT_PASSWORD = os.getenv("REDSHIFT_PASSWORD")

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


def get_batch_date_from_redshift():
    """Fetch latest batch_no and batch_date from devstage.batch_control."""
    conn = get_redshift_connection()
    cur = conn.cursor()

    query = """
        SELECT etl_batch_no, etl_batch_date
        FROM j25renu_etl_metadata.batch_control
        ORDER BY etl_batch_no DESC
        LIMIT 1;
    """
    cur.execute(query)
    row = cur.fetchone()
    cur.close()
    conn.close()

    if not row:
        raise ValueError(" No batch record found in batch_control table.")
    
    etl_batch_no, etl_batch_date = row
    print(f" Using ETL_BATCH_NO={etl_batch_no}, ETL_BATCH_DATE={etl_batch_date}")
    return etl_batch_no, etl_batch_date

def get_connection():
    """Connect to Oracle"""
    dsn = f"{host}:{port}/{service}"
    conn = oracledb.connect(user=user, password=password, dsn=dsn)
    return conn

def prepare_dblink(cursor,BATCH_DATE):
    BATCH_DATE = str(BATCH_DATE)
    if BATCH_DATE == "2001-01-01":
        remote_schema = "CM_20050609"
        remote_password = "CM_20050609123"
    else:
        remote_schema = f"CM_{BATCH_DATE.replace('-', '')}"
        remote_password = f"{remote_schema}123"

    linkName="test_dblink"
    cursor.execute("ALTER SESSION SET CURRENT_SCHEMA = j25Renu")
    try: 
        cursor.execute(f"DROP PUBLIC DATABASE LINK {linkName}")
    except Exception: 
        pass
    sql = f"""
    CREATE PUBLIC DATABASE LINK {linkName}
    CONNECT TO {remote_schema} IDENTIFIED BY "{remote_password}"
    USING '(DESCRIPTION=
      (ADDRESS=(PROTOCOL=TCP)(HOST={host})(PORT={port}))
      (CONNECT_DATA=(SERVICE_NAME={service}))
    )'
    """
    cursor.execute(sql)
    print(f"DBLink created for {remote_schema}")
    return linkName