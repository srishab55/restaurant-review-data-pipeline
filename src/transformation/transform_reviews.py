import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, trim, round as spark_round, lit
from datetime import datetime

# Constants
INPUT_BASE_PATH = "data/interim/reviews"
OUTPUT_BASE_PATH = "data/transformed/reviews_output"

# Expected schema
expected_columns = {
    "Id": IntegerType(),
    "ProductId": StringType(),
    "UserId": StringType(),
    "ProfileName": StringType(),
    "HelpfulnessNumerator": IntegerType(),
    "HelpfulnessDenominator": IntegerType(),
    "Score": IntegerType(),
    "Summary": StringType(),
    "Text": StringType()
}

# Configure logging for better tracking
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def validate_and_cast_schema(df):
    """
    Validate and cast columns to the expected schema types.
    """
    for col_name, expected_type in expected_columns.items():
        if col_name not in df.columns:
            raise ValueError(f"Missing required column: {col_name}")
        
        actual_type = df.schema[col_name].dataType
        if not isinstance(actual_type, type(expected_type)):
            logger.warning(f"Column '{col_name}' type mismatch: expected {expected_type}, got {actual_type}. Casting...")
            df = df.withColumn(col_name, col(col_name).cast(expected_type))

    logger.info("Schema validated and casted successfully.")
    return df

def perform_data_cleaning(df):
    """
    Perform data cleaning tasks such as trimming whitespace, handling missing values, and normalizing scores.
    """
    df = df.withColumn("Summary", trim(col("Summary"))) \
           .withColumn("Text", trim(col("Text")))

    # Normalize score (range 1-5 to 0-1)
    df = df.withColumn("score_normalized", spark_round((col("Score") - 1) / 4, 3))

    # Handle missing values by filling them with default values
    df = df.fillna({
        "ProfileName": "Unknown",
        "HelpfulnessNumerator": 0,
        "HelpfulnessDenominator": 0,
        "Summary": "No summary provided",
        "Text": "No text provided"
    })

    # Remove rows where Score is missing or out of expected range (1 to 5)
    df = df.filter((col("Score").isNotNull()) & (col("Score") >= 1) & (col("Score") <= 5))

    # Replace zero or negative values in HelpfulnessNumerator/Denominator
    df = df.withColumn("HelpfulnessNumerator", col("HelpfulnessNumerator").cast(IntegerType())) \
           .withColumn("HelpfulnessDenominator", col("HelpfulnessDenominator").cast(IntegerType()))
    df = df.withColumn("HelpfulnessNumerator", 
                       col("HelpfulnessNumerator").alias("HelpfulnessNumerator").cast(IntegerType()))
    df = df.withColumn("HelpfulnessDenominator", 
                       col("HelpfulnessDenominator").alias("HelpfulnessDenominator").cast(IntegerType()))

    logger.info("Data cleaning completed.")
    return df

def transform(df):
    """
    Apply transformations to the DataFrame.
    """
    df = perform_data_cleaning(df)
    return df

def run_transformation():
    """
    Main function to run the data transformation pipeline.
    """
    # Create a Spark session
    spark = SparkSession.builder.appName("TransformReviews").getOrCreate()

    # Read input data
    input_folders = [f.path for f in os.scandir(INPUT_BASE_PATH) if f.is_dir()]
    if not input_folders:
        logger.warning("No input folders found in the specified directory.")
        return

    for folder in input_folders:
        logger.info(f"Processing folder: {folder}")
        try:
            df = spark.read.parquet(folder)
        except Exception as e:
            logger.error(f"Failed to read parquet file from {folder}: {str(e)}")
            continue

        # Validate and transform the data
        try:
            df = validate_and_cast_schema(df)
            df_transformed = transform(df)
        except Exception as e:
            logger.error(f"Error during transformation in folder {folder}: {str(e)}")
            continue

        # Output path based on date from folder name
        date_str = folder.split("date=")[-1]
        output_path = os.path.join(OUTPUT_BASE_PATH, f"date={date_str}")

        # Write the transformed data to output location
        try:
            df_transformed.write.parquet(output_path, mode="overwrite")
            logger.info(f"Wrote transformed data to {output_path}")
        except Exception as e:
            logger.error(f"Failed to write transformed data to {output_path}: {str(e)}")

if __name__ == "__main__":
    run_transformation()