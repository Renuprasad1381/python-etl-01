import pandas as pd
import os
import io
import oracledb
import sys
import boto3
from dotenv import load_dotenv
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db_utils import get_connection,prepare_dblink,get_batch_date_from_redshift

# Load environment variables
load_dotenv()

TABLE="productlines"
productlinesColumn =os.getenv("productlinesColumn")
S3_BUCKET_NAME=os.getenv("S3_BUCKET_NAME")

def upload_to_s3(df, bucket_name, s3_key):

    # Convert DataFrame to CSV in memory
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    s3_client = boto3.client("s3")
    s3_client.put_object(Bucket=bucket_name, Key=s3_key, Body=csv_buffer.getvalue())
    print(f"Successfully uploaded {s3_key} to S3 bucket '{bucket_name}'")


def productlines():
    print("Connecting to Oracle...")
    BATCH_NUMBER,BATCH_DATE= get_batch_date_from_redshift()
    conn = get_connection()
    cur = conn.cursor()
    prepare_dblink(cur,BATCH_DATE)

    query = f"""
        SELECT
            {productlinesColumn}
        FROM {TABLE}@test_dblink
        WHERE UPDATE_TIMESTAMP >= TO_DATE('{BATCH_DATE}','YYYY-MM-DD')
    """

    df = pd.read_sql_query(query, conn,dtype_backend ="pyarrow")
    print(f"Fetched {len(df)} rows from {TABLE}@test_dblink")

    s3_key = f"{TABLE.upper()}/{BATCH_DATE}/{TABLE}.csv"
    upload_to_s3(df, S3_BUCKET_NAME, s3_key)

    conn.close()
    print("Connection closed.")


if __name__ == "__main__":
    productlines()