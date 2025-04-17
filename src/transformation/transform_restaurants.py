import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lower, regexp_replace, when, split
from pyspark.sql import DataFrame

# ------------------ Config ------------------
INPUT_DIR = "data/processed/flattened_restaurants"
OUTPUT_DIR = "data/transformed"
SPARK_CONFIG = {
    "spark.executor.memory": "2g",
    "spark.driver.memory": "1g",
    "spark.sql.shuffle.partitions": "4"
}
LOG_LEVEL = logging.INFO
# -------------------------------------------

# Setup logging
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def create_spark_session(app_name="Restaurant Data Transformation") -> SparkSession:
    """
    Creates and returns a Spark session with the given app name and configuration.
    """
    try:
        spark_builder = SparkSession.builder.appName(app_name)
        for key, value in SPARK_CONFIG.items():
            spark_builder = spark_builder.config(key, value)
        spark = spark_builder.getOrCreate()
        logger.info(f"Spark session created with app name: {app_name}")
        return spark
    except Exception as e:
        logger.error(f"Failed to create Spark session: {e}", exc_info=True)
        raise

def transform_restaurant_data(spark: SparkSession, input_dir: str, output_dir: str) -> None:
    """
    Transforms restaurant data, performs cleaning, and writes the result to the output directory.
    """
    try:
        logger.info("Reading Parquet data...")
        df = spark.read.parquet(os.path.join(input_dir, "*/*"))
        logger.info("Parquet data read successfully.")
        df.printSchema()

        df_cleaned = clean_data(df)
        
        output_parquet_path = os.path.join(output_dir, "restaurants_output")
        logger.info(f"Writing transformed data to: {output_parquet_path}")
        df_cleaned.repartition(5).write.mode("overwrite").parquet(output_parquet_path)
        logger.info(f"Cleaned and transformed data written to: {output_parquet_path}")

    except Exception as e:
        logger.error(f"Failed during transformation: {e}", exc_info=True)

def clean_data(df: DataFrame) -> DataFrame:
    """
    Cleans and transforms the raw restaurant DataFrame by renaming, trimming, filling missing values,
    and applying additional transformations.
    """
    try:
        df_cleaned = df.withColumnRenamed("id", "city_name") \
            .withColumnRenamed("rating", "rating_score") \
            .withColumnRenamed("rating_count", "rating_count_text") \
            .withColumn("restaurant_name", trim(lower(col("name")))) \
            .withColumn("rating_score", trim(col("rating_score"))) \
            .withColumn("rating_count_text", trim(col("rating_count_text"))) \
            .withColumn("cost_text", trim(col("cost"))) \
            .withColumn("restaurant_address", trim(lower(col("address")))) \
            .withColumn("licence_number", trim(col("lic_no"))) \
            .drop("name", "cost", "address", "lic_no")

        df_cleaned = df_cleaned.fillna({
            "restaurant_name": "unknown",
            "rating_score": "0.0",
            "rating_count_text": "0 ratings",
            "cost_text": "N/A",
            "restaurant_address": "unknown",
            "licence_number": "N/A"
        })

        df_cleaned = df_cleaned.dropDuplicates(["restaurant_id"])

        # Clean and transform cost_text and other columns
        df_cleaned = df_cleaned.withColumn("cost_numeric",
            when(col("cost_text").rlike("₹\\s*\\d+"),
                regexp_replace(col("cost_text"), "[^\\d.]", "").cast("double"))
            .otherwise(None))

        df_cleaned = df_cleaned.withColumn("restaurant_name",
                                           regexp_replace(col("restaurant_name"), "[^a-zA-Z0-9 ]", ""))

        df_cleaned = df_cleaned.withColumn("cuisine_list", split(col("cuisine"), ","))

        return df_cleaned
    except Exception as e:
        logger.error(f"Failed during data cleaning: {e}", exc_info=True)
        raise

def main():
    try:
        if not os.path.exists(OUTPUT_DIR):
            os.makedirs(OUTPUT_DIR)
        
        spark = create_spark_session()
        transform_restaurant_data(spark, INPUT_DIR, OUTPUT_DIR)
        spark.stop()

    except Exception as e:
        logger.error(f"Failed in the main execution: {e}", exc_info=True)

if __name__ == "__main__":
    main()