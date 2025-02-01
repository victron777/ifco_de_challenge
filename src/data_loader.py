import logging
from pyspark.sql.functions import from_json, col, explode, regexp_replace, when, lit
from src.utils import contact_schema

# Create a logger
logger = logging.getLogger(__name__)
# logger.setLevel(logging.DEBUG)

def load_data(spark, orders_path, invoicing_path):
    """
    Load the data from CSV and JSON files into Spark DataFrames.
    """

    # Load orders.csv
    orders_df = process_orders(spark, orders_path)

    # Show loaded datasets
    logger.info(f"Count of processed orders_df:: {orders_df.count()}")
    logger.debug(f"processed orders_df DF:\n{orders_df.show(truncate=False)}")

    invoicing_input = spark.read.option("multiline", "true").json(invoicing_path) #spark.read.json(invoicing_path)
    invoicing_df = invoicing_input.selectExpr("explode(data.invoices) as invoice").select("invoice.*")

    # Show loaded datasets
    logger.info(f"Count of processed invoicing_df:: {invoicing_df.count()}")
    logger.debug(f"processed invoicing_df DF:\n{invoicing_df.show(truncate=False)}")

    return orders_df, invoicing_df

def process_orders(spark, orders_path):

    """
    Load and process orders data from CSV into a clean Spark DataFrame.
    :param spark, orders_path:
    :return: orders_df
    """

    # Load orders.csv
    orders_df = spark.read.csv(path=orders_path, sep=";", header=True, inferSchema=True)
    # orders_df.show(truncate=False)
    logger.info(f"Count of original orders_df:: {orders_df.count()}")
    logger.debug(f"original orders_df DF:\n{orders_df.show(truncate=False)}")

    cleaned_df = (orders_df
                  .withColumn("contact_data_fix_edge_quotes", regexp_replace(col("contact_data"), '^"|"$', ''))
                  .withColumn("contact_data_2", regexp_replace(col("contact_data_fix_edge_quotes"), '""', '"'))
                  )

    logger.info(f"count of cleaned_df:: {cleaned_df.count()}")
    logger.debug(f"cleaned_df DF:\n{cleaned_df.show(truncate=False)}")

    valid_contacts_df = (
        cleaned_df
        .withColumn(
            "right_format",
            when(
                col("contact_data_2").rlike(r"^\[.*\]"),  # Check if it looks like a valid array
                lit(1)
            ).otherwise(lit(0)))
        .withColumn(
            "bad_format_reason",
            when(~col("contact_data_2").rlike(r"^\[.*\]"), lit('Bad format')) # Check if it looks like a valid array
            .when(col("contact_data_2").contains("NULL"), lit('Null contact_data')) # Check if it contains null values
            .when(col("contact_data_2").isNull(), lit('Null contact_data')) # Check if it contains null values
            .otherwise(lit(''))
        )
    )

    logger.info(f"count of valid_contacts_df:: {valid_contacts_df.count()}")
    logger.debug(f"valid_contacts_df DF:\n{valid_contacts_df.show(truncate=False)}")

    # Explode the parsed contact data and handle nulls in 'cp'
    exploded_valid_df = (
        valid_contacts_df
        .withColumn(
            "parsed_contact_data",
            when(col("right_format") == 1,
                 from_json(col("contact_data_2"), contact_schema)
                 ).otherwise(lit(None))
        )
        .withColumn("contact", explode(when(col("right_format") == 1, col("parsed_contact_data")).otherwise(lit([None]))))
        .select(
            "order_id",
            "date",
            "company_id",
            "company_name",
            "crate_type",
            "salesowners",
            "contact_data_2",
            "right_format",
            "bad_format_reason",
            when(col("contact.contact_name").isNull(), "John").otherwise(col("contact.contact_name")).alias("contact_name"),
            when(col("contact.contact_surname").isNull(), "Doe").otherwise(col("contact.contact_surname")).alias("contact_surname"),
            when(col("contact.city").isNull(), "Unknown").otherwise(col("contact.city")).alias("city"),
            when(col("contact.cp").isNull(), "UNK00").otherwise(col("contact.cp")).alias("cp")
        )
    )

    logger.info(f"count of exploded_valid_df:: {exploded_valid_df.count()}")
    logger.debug(f"exploded_valid_df DF:\n{exploded_valid_df.show(truncate=False)}")

    return exploded_valid_df

