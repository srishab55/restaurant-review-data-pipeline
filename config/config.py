# config/config.py

SPARK_CONFIG = {
    "spark.executor.memory": "2g",
    "spark.driver.memory": "1g",
}

INPUT_PATH = "data/processed/flattened_restaurants"
OUTPUT_PATH = "data/transformed"
LOG_PATH = "logs/transform.log"