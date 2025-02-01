from pyspark.sql import SparkSession
from src.config import ORDERS_FILE, INVOICING_FILE, OUTPUT_FOLDER
from src.data_loader import load_data
from src.analysis import *

def main():
    print("Hello, World!. Now with Spark running")

    # Initialize Spark session
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("IFCO Assessment") \
        .getOrCreate()

    ### TEMPORAL CODE ### TODO: REMOVE IT
    # Create a DataFrame
    data = [("Alice", 34), ("Bob", 45)]
    columns = ["Name", "Age"]
    df = spark.createDataFrame(data, columns)

    # Show the DataFrame
    print("DataFrame Contents:")
    df.show()
    ### TEMPORAL CODE ###

    # Load data
    orders_df, invoicing_df = load_data(spark, ORDERS_FILE, INVOICING_FILE)

    # Calculate crate type distribution
    distribution_df = calculate_crate_distribution(spark, orders_df)

    # Calculate orders with contact
    orders_contact_df = calculate_orders_with_contact(orders_df)

    # Calculate orders with contact address
    orders_contact_address_df = calculate_orders_contact_address(orders_df)

    # Calculate sales team commissions
    sales_comissions_df  = calculate_sales_commissions(orders_df, invoicing_df)

    # Calculate Companies with Sales Owners
    companies_with_sales_owners_df = calculate_companies_with_sales_owners(orders_df)

    # # Save the results to a CSV file
    # distribution_df.write.csv(config.OUTPUT_FOLDER, header=True, mode="overwrite")

    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    main()
