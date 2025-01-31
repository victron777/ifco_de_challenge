from pyspark.sql import SparkSession
from src.config import ORDERS_FILE, INVOICING_FILE, OUTPUT_FOLDER
from src.data_loader import load_data

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


    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    main()
