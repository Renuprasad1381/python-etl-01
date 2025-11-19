import os
import subprocess
import sys
from datetime import datetime
from db_utils import get_batch_date_from_redshift, get_redshift_connection

sys.stdout.reconfigure(encoding='utf-8')

# Step 1: Fetch batch info
etl_batch_no, etl_batch_date = get_batch_date_from_redshift()
print(f"Running ETL with ETL_BATCH_NO={etl_batch_no}, ETL_BATCH_DATE={etl_batch_date}")

# Step 2: Redshift connection
conn = get_redshift_connection()
cur = conn.cursor()

# Step 3: Insert or update log as RUNNING
start_time = datetime.now()
cur.execute(f"""
    SELECT COUNT(*) 
    FROM j25Renu_etl_metadata.batch_control_log
    WHERE etl_batch_no = {etl_batch_no}
      AND etl_batch_date = '{etl_batch_date}';
""")
exists = cur.fetchone()[0]

if exists > 0:
    cur.execute(f"""
        UPDATE j25Renu_etl_metadata.batch_control_log
        SET etl_batch_status = 'R',
            etl_batch_start_time = '{start_time}',
            etl_batch_end_time = NULL
        WHERE etl_batch_no = {etl_batch_no}
          AND etl_batch_date = '{etl_batch_date}';
    """)
    print("Existing ETL batch found — status reset to RUNNING.")
else:
    cur.execute(f"""
        INSERT INTO j25Renu_etl_metadata.batch_control_log (
            etl_batch_no,
            etl_batch_date,
            etl_batch_status,
            etl_batch_start_time
        )
        VALUES ({etl_batch_no}, '{etl_batch_date}', 'R', '{start_time}');
    """)
    print("Logged new ETL batch start (Running).")

conn.commit()

# Step 4: Run ETL stage scripts sequentially (inside their folders)
etl_failed = False
scripts = [
    ("source_to_s3", "main.py"),
    ("s3_to_devstage", "main.py"),
    ("devstage_to_devdw", "main.py")
]

for folder, script_name in scripts:
    script_path = os.path.join(folder, script_name)
    print(f"\n▶ Running {script_path} ...")

    # Run inside the script's folder
    result = subprocess.run([sys.executable, script_name], cwd=folder)

    if result.returncode != 0:
        etl_failed = True
        print(f"❌ FAILED: {script_path}")
        break
    else:
        print(f"✅ Completed: {script_path}")

# Step 5: Update ETL status in Redshift log
end_time = datetime.now()
status = 'F' if etl_failed else 'C'

cur.execute(f"""
    UPDATE j25Renu_etl_metadata.batch_control_log
    SET etl_batch_status = '{status}',
        etl_batch_end_time = '{end_time}'
    WHERE etl_batch_no = {etl_batch_no}
      AND etl_batch_date = '{etl_batch_date}';
""")

conn.commit()
cur.close()
conn.close()

print(f"\nETL process {'FAILED' if etl_failed else 'COMPLETED SUCCESSFULLY'}")
print(f"Redshift connection closed. Final ETL Status: {status}")
