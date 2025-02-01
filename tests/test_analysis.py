import unittest
from src.analysis import  *
import os
from pyspark.sql import SparkSession

class TestMain(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """
        Set up a Spark session for testing.
        """
        cls.spark = SparkSession.builder \
            .master("local[*]") \
            .appName("unittest-pyspark") \
            .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        """
        Stop the Spark session after tests are done.
        """
        cls.spark.stop()

    def test_calculate_crate_distribution(self):
        """
        Test the calculate_crate_distribution method to ensure it correctly calculates
        the distribution of crate types per company.
        """
        # Create test data
        data = [
            ("Company A", "Crate Type 1"),
            ("Company A", "Crate Type 1"),
            ("Company A", "Crate Type 2"),
            ("Company B", "Crate Type 1"),
            ("Company B", "Crate Type 2"),
            ("Company B", "Crate Type 2"),
        ]
        columns = ["company_name", "crate_type"]
        orders_df = self.spark.createDataFrame(data, columns)

        # Expected result
        expected_data = [
            ("Company A", "Crate Type 1", 2),  # Company A has 2 orders of Crate Type 1
            ("Company A", "Crate Type 2", 1),  # Company A has 1 order of Crate Type 2
            ("Company B", "Crate Type 1", 1),  # Company B has 1 order of Crate Type 1
            ("Company B", "Crate Type 2", 2),  # Company B has 2 orders of Crate Type 2
        ]
        expected_columns = ["company_name", "crate_type", "order_count"]
        expected_df = self.spark.createDataFrame(expected_data, expected_columns)

        # Call the method to test
        result_df = calculate_crate_distribution(self.spark, orders_df)

        # Compare the result with the expected output
        self.assertEqual(result_df.collect(), expected_df.collect())


    def test_calculate_orders_with_contact_missing_info(self):
        """
        Test the calculate_orders_with_contact method with missing contact information.
        """
        # Create test data
        data = [
            (1, "John", "Doe"),  # Complete contact information
            (2, "Jane", None),   # Missing surname
            (3, None, "Smith"),  # Missing name
            (4, None, None)      # Missing both name and surname
        ]
        columns = ["order_id", "contact_name", "contact_surname"]
        orders_df = self.spark.createDataFrame(data, columns)

        # Expected result
        expected_data = [
            (1, "John Doe"),     # Complete contact information
            (2, "John Doe"),     # Missing surname → placeholder
            (3, "John Doe"),     # Missing name → placeholder
            (4, "John Doe")      # Missing both → placeholder
        ]
        expected_columns = ["order_id", "contact_full_name"]
        expected_df = self.spark.createDataFrame(expected_data, expected_columns)

        # Call the method to test
        result_df = calculate_orders_with_contact(orders_df)

        # Compare the result with the expected output
        self.assertEqual(result_df.collect(), expected_df.collect())

    def test_calculate_orders_contact_address(self):
        """
        Test the calculate_orders_contact_address method to ensure it correctly formats
        the contact address and handles missing city or postal code information.
        """
        # Create test data
        data = [
            (1, "New York", "10001"),  # Complete address information
            (2, None, "20002"),        # Missing city
            (3, "Los Angeles", None),  # Missing postal code
            (4, None, None)            # Missing both city and postal code
        ]
        columns = ["order_id", "city", "cp"]
        orders_df = self.spark.createDataFrame(data, columns)

        # Expected result
        expected_data = [
            (1, "New York 10001"),     # Complete address information
            (2, "Unknown 20002"),      # Missing city → "Unknown"
            (3, "Los Angeles UNK00"),  # Missing postal code → "UNK00"
            (4, "Unknown UNK00")       # Missing both → "Unknown UNK00"
        ]
        expected_columns = ["order_id", "contact_address"]
        expected_df = self.spark.createDataFrame(expected_data, expected_columns)

        # Call the method to test
        result_df = calculate_orders_contact_address(orders_df)

        # Compare the result with the expected output
        self.assertEqual(result_df.collect(), expected_df.collect())

    def test_calculate_sales_commissions(self):
        """
        Test the calculate_sales_commissions method to ensure it correctly calculates
        commissions for sales owners and sorts the results in descending order.
        """
        # Create test data for orders
        orders_data = [
            (1, "Alice, Bob, Charlie"),  # Order 1 with 3 salesowners
            (2, "Alice, Dave"),          # Order 2 with 2 salesowners
            (3, "Eve")                   # Order 3 with 1 salesowner
        ]
        orders_columns = ["order_id", "salesowners"]
        orders_df = self.spark.createDataFrame(orders_data, orders_columns)

        # Create test data for invoices
        invoices_data = [
            (1, 10000),  # Order 1: grossValue = 10000 cents (100.00 euros)
            (2, 5000),   # Order 2: grossValue = 5000 cents (50.00 euros)
            (3, 2000)    # Order 3: grossValue = 2000 cents (20.00 euros)
        ]
        invoices_columns = ["orderId", "grossValue"]
        invoices_df = self.spark.createDataFrame(invoices_data, invoices_columns)

        # Expected result
        expected_data = [
            ("Alice", 9.00),   # Main owner for Order 1 (6% of 100.00) + Main owner for Order 2 (6% of 50.00)
            ("Bob", 2.50),     # Co-owner 1 for Order 1 (2.5% of 100.00)
            ("Charlie", 0.95), # Co-owner 2 for Order 1 (0.95% of 100.00)
            ("Dave", 1.25),    # Co-owner 1 for Order 2 (2.5% of 50.00)
            ("Eve", 1.20)      # Main owner for Order 3 (6% of 20.00)
        ]
        expected_columns = ["salesowner", "total_commission"]
        expected_df = self.spark.createDataFrame(expected_data, expected_columns)

        # Call the method to test
        result_df = calculate_sales_commissions(orders_df, invoices_df)

        # Sort both DataFrames by salesowner for consistent comparison
        result_df = result_df.orderBy("salesowner")
        expected_df = expected_df.orderBy("salesowner")

        # Compare the result with the expected output
        self.assertEqual(result_df.collect(), expected_df.collect())

    def test_calculate_companies_with_sales_owners(self):
        """
        Test the calculate_companies_with_sales_owners method to ensure it correctly
        handles duplicate companies, creates a unique and sorted list of salesowners,
        and returns the expected DataFrame.
        """
        # Create test data for orders
        orders_data = [
            (1, "Company A", "Alice, Bob"),  # Company A with 2 salesowners
            (2, "Company A", "Alice, Charlie"),  # Duplicate company with different ID
            (3, "Company B", "Dave"),  # Company B with 1 salesowner
            (4, "Company C", "Eve, Frank"),  # Company C with 2 salesowners
            (5, "Company C", "Eve, Frank")  # Duplicate company with different ID
        ]
        orders_columns = ["company_id", "company_name", "salesowners"]
        orders_df = self.spark.createDataFrame(orders_data, orders_columns)

        # Expected result
        expected_data = [
            (1, "Company A", "Alice, Bob, Charlie"),  # Company A with unique and sorted salesowners
            (3, "Company B", "Dave"),  # Company B with 1 salesowner
            (4, "Company C", "Eve, Frank")  # Company C with unique and sorted salesowners
        ]
        expected_columns = ["company_id", "company_name", "list_salesowners"]
        expected_df = self.spark.createDataFrame(expected_data, expected_columns)

        # Call the method to test
        result_df = calculate_companies_with_sales_owners(orders_df)

        # Sort both DataFrames by company_id for consistent comparison
        result_df = result_df.orderBy("company_id")
        expected_df = expected_df.orderBy("company_id")

        # Compare the result with the expected output
        self.assertEqual(result_df.collect(), expected_df.collect())

# Run the tests
if __name__ == "__main__":
    unittest.main()