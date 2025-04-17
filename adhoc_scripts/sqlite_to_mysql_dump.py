import sqlite3
import mysql.connector
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# SQLite and MySQL configurations
SQLITE_DB_PATH = "data/raw/reviews.sqlite"
MYSQL_CONFIG = {
    "host": "localhost",  # Or replace with 'mysql_reviews' if using Docker
    "port": 3307,  # Ensure this is correctly mapped in Docker-compose
    "user": "review_user",
    "password": "review_pass",
    "database": "reviews_db",
    "auth_plugin": "mysql_native_password",  # Ensure correct plugin
    "connection_timeout": 300  # Increased connection timeout to 5 minutes
}
MYSQL_TABLE_NAME = "Reviews"
BATCH_SIZE = 10000  # Number of rows to fetch and insert per batch

def dump_sqlite_to_mysql():
    try:
        # Connect to SQLite
        sqlite_conn = sqlite3.connect(SQLITE_DB_PATH)
        sqlite_cursor = sqlite_conn.cursor()
        logging.info("Connected to SQLite DB.")

        # Connect to MySQL
        mysql_conn = mysql.connector.connect(**MYSQL_CONFIG)
        mysql_cursor = mysql_conn.cursor()
        logging.info("Connected to MySQL DB.")

        # Fetch the total number of rows in the SQLite table to manage batch processing
        sqlite_cursor.execute("SELECT COUNT(*) FROM Reviews")
        total_rows = sqlite_cursor.fetchone()[0]
        logging.info(f"Total records in SQLite: {total_rows}")

        # Batch loading
        offset = 0
        while offset < total_rows:
            # Fetch a batch of rows from SQLite
            sqlite_cursor.execute(f"SELECT * FROM Reviews LIMIT {BATCH_SIZE} OFFSET {offset}")
            rows = sqlite_cursor.fetchall()
            logging.info(f"Fetched {len(rows)} records from SQLite (offset: {offset}).")

            # Insert into MySQL
            insert_query = f"""
            INSERT INTO {MYSQL_TABLE_NAME}
            (Id, ProductId, UserId, ProfileName, HelpfulnessNumerator,
            HelpfulnessDenominator, Score, Time, Summary, Text)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            mysql_cursor.executemany(insert_query, rows)
            mysql_conn.commit()
            logging.info(f"Inserted {len(rows)} records into MySQL.")

            # Increment offset for the next batch
            offset += BATCH_SIZE

        logging.info("Data dump completed successfully.")

    except Exception as e:
        logging.error(f"Error occurred: {e}")
    finally:
        sqlite_conn.close()
        mysql_conn.close()
        logging.info("Connections closed.")

if __name__ == "__main__":
    dump_sqlite_to_mysql()