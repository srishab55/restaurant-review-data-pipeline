import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col
from transform_restaurants import clean_data, create_spark_session

class TestTransformRestaurants(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Set up Spark session for testing
        cls.spark = SparkSession.builder.master("local[1]").appName("TestTransformRestaurants").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_clean_data(self):
        # Sample test schema and data for cleaning
        test_schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("rating", StringType(), True),
            StructField("rating_count", StringType(), True),
            StructField("cost", StringType(), True),
            StructField("address", StringType(), True),
            StructField("lic_no", StringType(), True),
            StructField("cuisine", StringType(), True)
        ])
        
        test_data = [(1, "Restaurant A", "4", "100", "₹200", "Address A", "LIC123", "Italian, Pizza")]
        df = self.spark.createDataFrame(test_data, test_schema)

        # Apply cleaning function
        cleaned_df = clean_data(df)

        # Validate transformed columns
        self.assertEqual(cleaned_df.select("restaurant_name").collect()[0][0], "restaurant a")
        self.assertEqual(cleaned_df.select("cost_numeric").collect()[0][0], 200.0)
        self.assertEqual(cleaned_df.select("cuisine_list").collect()[0][0], ["Italian", "Pizza"])

    def test_create_spark_session(self):
        # Test if Spark session is created successfully
        spark = create_spark_session()
        self.assertIsNotNone(spark)

if __name__ == "__main__":
    unittest.main()