import unittest
from pyspark.sql import SparkSession
from src.main import main
from io import StringIO
import sys

class TestMain(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Initialize Spark session for testing
        cls.spark = SparkSession.builder \
            .master("local[*]") \
            .appName("TestApp") \
            .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        # Stop the Spark session after tests are done
        cls.spark.stop()

    def test_main(self):
        # Capture the output of main()
        captured_output = StringIO()
        sys.stdout = captured_output
        main()
        sys.stdout = sys.__stdout__

        # Check if the output contains the DataFrame contents
        output = captured_output.getvalue().strip()
        self.assertIn("DataFrame Contents:", output)
        self.assertIn("Alice", output)
        self.assertIn("Bob", output)

if __name__ == "__main__":
    unittest.main()