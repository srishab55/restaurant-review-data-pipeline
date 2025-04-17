import os
import pandas as pd
import pymysql
from datetime import datetime
import sys
from airflow.providers.mysql.hooks.mysql import MySqlHook

from config.db_config import MYSQL_CONFIG

# Where we store the last processed epoch timestamp
LAST_PROCESSED_FILE = "data/temporary/last_processed.txt"
OUTPUT_BASE_PATH = "data/interim/reviews"

def get_mysql_connection():
    hook = MySqlHook(mysql_conn_id='mysql_reviews')
    return hook.get_conn()

def read_last_processed():
    if os.path.exists(LAST_PROCESSED_FILE):
        with open(LAST_PROCESSED_FILE, "r") as f:
            content = f.read().strip()
            if content:
                try:
                    return int(content)
                except ValueError:
                    print("Invalid timestamp in file. Deleting and falling back")
            else:
                print(" Empty timestamp file. Deleting and falling back")
        os.remove(LAST_PROCESSED_FILE)

    # Fallback to earliest Time value from MySQL
    try:
        conn = get_mysql_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT MIN(Time) FROM Reviews where extract( year from from_unixtime(time)) = 2012")
        result = cursor.fetchone()[0]
        conn.close()
        print(f"Fallback to earliest Time value: {result}")
        return result if result else int(datetime.now().timestamp()) - 3600
    except Exception as e:
        print(f"Error fetching fallback timestamp: {e}")
        return int(datetime.now().timestamp()) - 86400

def write_last_processed(ts: int):
    os.makedirs(os.path.dirname(LAST_PROCESSED_FILE), exist_ok=True)
    with open(LAST_PROCESSED_FILE, "w") as f:
        f.write(str(ts))

def fetch_and_dump(start_ts: int):
    end_ts = start_ts + 86400

    print(f"Fetching records with Time between {start_ts} and {end_ts}")
    conn = get_mysql_connection()
    query = """
        SELECT * FROM Reviews
        WHERE Time >= %s AND Time < %s
        and extract( year from from_unixtime(time)) = 2012
    """
    df = pd.read_sql(query, conn, params=(start_ts, end_ts))
    conn.close()


    if df.empty:
        print("No new records found.")
        return False, end_ts

    # Convert epoch to datetime for folder naming
    dt = datetime.utcfromtimestamp(start_ts)
    date_str = dt.strftime("%Y-%m-%d")

    folder = os.path.join(
        OUTPUT_BASE_PATH,
        f"date={date_str}"
    )
    os.makedirs(folder, exist_ok=True)

    output_file = os.path.join(folder, "data.parquet")
    df.to_parquet(output_file, index=False)
    print(f"Wrote {len(df)} records to {output_file}")
    return True, end_ts

def run_incremental_job():
    last_ts = read_last_processed()
    success, new_ts = fetch_and_dump(last_ts)

    if success:
        write_last_processed(new_ts)

if __name__ == "__main__":
    run_incremental_job()