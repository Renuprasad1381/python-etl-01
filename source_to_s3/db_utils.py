import os
import oracledb
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

user = os.getenv("ORACLE_USER")
password = os.getenv("ORACLE_PASSWORD")
host = os.getenv("ORACLE_HOST")
port = os.getenv("ORACLE_PORT")        
service = os.getenv("ORACLE_SERVICE")

def get_connection():
    """Connect to Oracle"""
    dsn = f"{host}:{port}/{service}"
    conn = oracledb.connect(user=user, password=password, dsn=dsn)
    return conn

def prepare_dblink(cursor,BATCH_DATE):
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
