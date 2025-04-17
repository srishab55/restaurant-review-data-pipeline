import unittest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from transform_reviews import validate_and_cast_schema, transform

class TestTransformReviews(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Set up Spark session for testing
        cls.spark = SparkSession.builder.master("local[1]").appName("TestTransformReviews").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_validate_and_cast_schema_valid(self):
        test_schema = StructType([
            StructField("Id", IntegerType(), True),
            StructField("ProductId", StringType(), True),
            StructField("UserId", StringType(), True),
            StructField("ProfileName", StringType(), True),
            StructField("HelpfulnessNumerator", IntegerType(), True),
            StructField("HelpfulnessDenominator", IntegerType(), True),
            StructField("Score", IntegerType(), True),
            StructField("Summary", StringType(), True),
            StructField("Text", StringType(), True),
        ])
        
        test_data = [(1, "P1", "U1", "User 1", 3, 5, 4, "Good product", "Nice product")]
        df = self.spark.createDataFrame(test_data, test_schema)

        # Validate schema
        result_df = validate_and_cast_schema(df)

        # Assert all columns are present and casted properly
        self.assertEqual(len(result_df.columns), len(test_schema.fields))

    def test_transform_function(self):
        test_schema = StructType([
            StructField("Id", IntegerType(), True),
            StructField("ProductId", StringType(), True),
            StructField("UserId", StringType(), True),
            StructField("ProfileName", StringType(), True),
            StructField("HelpfulnessNumerator", IntegerType(), True),
            StructField("HelpfulnessDenominator", IntegerType(), True),
            StructField("Score", IntegerType(), True),
            StructField("Summary", StringType(), True),
            StructField("Text", StringType(), True),
        ])
        
        test_data = [(1, "P1", "U1", "User 1", 3, 5, 4, " Good summary ", "Nice product")]
        df = self.spark.createDataFrame(test_data, test_schema)

        # Apply transformations
        transformed_df = transform(df)

        # Check if the 'score_normalized' column is created correctly
        transformed_score = transformed_df.collect()[0]["score_normalized"]
        self.assertEqual(transformed_score, 0.75)

    def test_perform_data_cleaning(self):
        test_schema = StructType([
            StructField("Id", IntegerType(), True),
            StructField("ProductId", StringType(), True),
            StructField("UserId", StringType(), True),
            StructField("ProfileName", StringType(), True),
            StructField("HelpfulnessNumerator", IntegerType(), True),
            StructField("HelpfulnessDenominator", IntegerType(), True),
            StructField("Score", IntegerType(), True),
            StructField("Summary", StringType(), True),
            StructField("Text", StringType(), True),
        ])
        
        test_data = [(1, "P1", "U1", "User 1", 3, 5, 4, "  Product summary   ", "Nice product")]
        df = self.spark.createDataFrame(test_data, test_schema)

        # Perform cleaning
        cleaned_df = transform(df)

        # Assert that Summary and Text columns are trimmed
        self.assertEqual(cleaned_df.collect()[0]["Summary"], "Product summary")
        self.assertEqual(cleaned_df.collect()[0]["Text"], "Nice product")

if __name__ == "__main__":
    unittest.main()