from pyspark.sql.types import ArrayType, StructType, StructField, StringType, IntegerType

# Define contact schema used in orders input data
contact_schema = ArrayType(
    StructType([
        StructField("contact_name", StringType(), True),
        StructField("contact_surname", StringType(), True),
        StructField("city", StringType(), True),
        StructField("cp", StringType(), True),
    ])
)