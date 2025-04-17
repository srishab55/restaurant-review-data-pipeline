import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
import json

print("Starting JSON to Parquet conversion...")
# Initialize Spark session
spark = SparkSession.builder \
    .appName("FlattenRestaurantsJSON") \
    .master("local[*]") \
    .getOrCreate()

# Define input path and output path
input_dir = "data/interim/restaurants"  # Folder containing chunked JSON files
output_dir = "data/processed/flattened_restaurants"  # Folder for processed Parquet

# Function to process each JSON chunk file
def process_json_file(input_path):
    with open(input_path, "r", encoding="utf-8") as f:
        raw_data_list = json.load(f)

    print(f"Loaded {len(raw_data_list)} records from {input_path}")
    # Flatten the records
    records = []
    for entry in raw_data_list:
        record_id = entry.get("id")
        record_link = entry.get("link")
        restaurants = entry.get("restaurants", {})

        for restaurant_id, restaurant_data in restaurants.items():
            records.append({
                "id": record_id,
                "link": record_link,
                "restaurant_id": restaurant_id,
                "name": restaurant_data.get("name"),
                "rating": restaurant_data.get("rating"),
                "rating_count": restaurant_data.get("rating_count"),
                "cost": restaurant_data.get("cost"),
                "address": restaurant_data.get("address"),
                "cuisine": restaurant_data.get("cuisine"),
                "lic_no": restaurant_data.get("lic_no"),
                "menu": json.dumps(restaurant_data.get("menu", {}))  # Keep menu as stringified JSON
            })

    # Define schema
    restaurant_schema = StructType([
        StructField("id", StringType(), True),
        StructField("link", StringType(), True),
        StructField("restaurant_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("rating", StringType(), True),
        StructField("rating_count", StringType(), True),
        StructField("cost", StringType(), True),
        StructField("address", StringType(), True),
        StructField("cuisine", StringType(), True),
        StructField("lic_no", StringType(), True),
        StructField("menu", StringType(), True),
    ])

    # Create Spark DataFrame
    df = spark.createDataFrame(records, schema=restaurant_schema)
    print(f"DataFrame created with {df.count()} records.")
    df.printSchema()

    # Write to Parquet (append mode to handle multiple chunks)
    output_file_path = os.path.join(output_dir, f"{os.path.basename(input_path.replace("json","").replace(".",""))}")
    df.coalesce(1).write.mode("overwrite").parquet(output_file_path)

# Process all JSON files in the directory
for file_name in os.listdir(input_dir):
    if file_name.endswith(".json"):  # Process only JSON files
        input_file_path = os.path.join(input_dir, file_name)
        print(f"Processing {file_name}...")
        process_json_file(input_file_path)

print("All files processed successfully.")
spark.stop()