from pyspark.sql import SparkSession

def main():
    print("Hello, World!. Now with Spark running")

    # Initialize Spark session
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("HelloWorldApp") \
        .getOrCreate()

    # Create a DataFrame
    data = [("Alice", 34), ("Bob", 45)]
    columns = ["Name", "Age"]
    df = spark.createDataFrame(data, columns)

    # Show the DataFrame
    print("DataFrame Contents:")
    df.show()

    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    main()
