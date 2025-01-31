from pyspark.sql import SparkSession
import src.config as config
from src.data_loader import *

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
    orders_df, invoicing_df = load_data(spark, config.ORDERS_FILE, config.INVOICING_FILE)


    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    main()
