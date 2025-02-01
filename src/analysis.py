import logging
from src.data_loader import *
from pyspark.sql.functions import from_json, col, explode, regexp_replace, concat, when, lit, split, expr, sum as F_sum, round
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from src.utils import contact_schema

# Create a logger
logger = logging.getLogger(__name__)
# logger.setLevel(logging.DEBUG)

def calculate_crate_distribution(spark, orders_df):
    """
    Calculate the distribution of crate types per company.
    """
    logger.info(f"Starting to crate distribution calculation")

    distribution_df = orders_df.groupBy("company_name", "crate_type") \
        .agg(F.count("*").alias("order_count")) \
        .orderBy("company_name", "crate_type")

    # # Show results
    logger.info(f"Count of records in distribution_df:: {distribution_df.count()}")
    logger.debug(f"distribution_df DF:\n{distribution_df.show(truncate=False)}")

    return distribution_df

def calculate_orders_with_contact(orders_df):
    """
    Calculate order with contact information.
    If contact_name or contact_surname is missing, use "John Doe" as the placeholder.
    :param orders_df: Input DataFrame with order and contact details
    :return: DataFrame with order_id and contact_full_name
    """
    orders_df = orders_df.withColumn(
        "contact_full_name",
        when(
            (col("contact_name").isNotNull() & col("contact_surname").isNotNull()),
            concat(col("contact_name"), lit(' '), col("contact_surname"))
        ).otherwise(lit("John Doe"))  # Use "John Doe" if either name or surname is missing
    ).select("order_id", "contact_full_name")

    # # Show results
    logger.info(f"Count of records in orders with contact:: {orders_df.count()}")
    logger.debug(f"orders_df with contact DF:\n{orders_df.show(truncate=False)}")

    return orders_df

def calculate_orders_contact_address(orders_df):
    """
    Calculate order with contact address information
    :param spark:
    :param orders_df:
    :return: orders_df
    """
    orders_df = orders_df.withColumn(
        "contact_address",
        concat(
            when(col("city").isNotNull(), col("city")).otherwise(lit("Unknown")),  # Use "Unknown" if city is missing
            lit(" "),  # Add a space between city and postal code
            when(col("cp").isNotNull(), col("cp")).otherwise(lit("UNK00"))  # Use "UNK00" if postal code is missing
        )
    ).select("order_id", "contact_address")

    # # Show results
    logger.info(f"Count of records in orders with contact address:: {orders_df.count()}")
    logger.debug(f"orders_df with contact address DF:\n{orders_df.show(truncate=False)}")

    return orders_df

def calculate_sales_commissions(orders_df, invoices_df):
    # Join Orders and Invoices on order_id/orderId
    df = orders_df.join(invoices_df, orders_df.order_id == invoices_df.orderId, "inner").drop("orderId")

    # Split salesowners into an array
    df = df.withColumn("salesowners", split(col("salesowners"), ", "))

    # Explode salesowners to get individual salesperson entries
    df = df.withColumn("salesowner", explode(col("salesowners")))

    # Assign commission based on rank (position in the list)
    df = df.withColumn("rank", expr("array_position(salesowners, salesowner)"))

    # Apply commission rates
    df = df.withColumn(
        "commission",
        when(col("rank") == 1, col("grossValue") * 0.06)  # Main Owner (6%)
        .when(col("rank") == 2, col("grossValue") * 0.025)  # Co-owner 1 (2.5%)
        .when(col("rank") == 3, col("grossValue") * 0.0095)  # Co-owner 2 (0.95%)
        .otherwise(0)  # Others get nothing
    )

    # Convert commission from cents to euros and round to 2 decimal places
    df = df.withColumn("commission", (col("commission") / 100).cast("decimal(10,2)"))

    # # Group by sales owner and sum commissions
    final_df = df.groupBy("salesowner").agg(
        round(F_sum(col("commission").cast("double")), 2).alias("total_commission")
    ).orderBy(col("total_commission").desc())

    # # Show results
    logger.info(f"Count of records in sales commissions:: {final_df.count()}")
    logger.debug(f"sales commissions DF:\n{final_df.show(truncate=False)}")

    return final_df


def calculate_companies_with_sales_owners(orders_df):

    # Step 1: Normalize company names to handle duplicates (Optional)
    company_window = Window.partitionBy("company_name").orderBy("company_id")
    df = orders_df.withColumn("rank", F.row_number().over(company_window)) \
        .withColumn("normalized_company_id", F.first("company_id").over(company_window))

    # Step 2: Explode the salesowners list and remove duplicates
    df_salesowners = df.withColumn("salesowner", F.explode(F.split(F.col("salesowners"), ", "))) \
        .select("normalized_company_id", "company_name", "salesowner") \
        .dropDuplicates()

    # Step 3: Aggregate sales owners into a sorted, comma-separated list
    df_3 = df_salesowners.groupBy("normalized_company_id", "company_name") \
        .agg(F.concat_ws(", ", F.sort_array(F.collect_set("salesowner"))).alias("list_salesowners")) \
        .withColumnRenamed("normalized_company_id", "company_id")

    # # Show results
    logger.info(f"Count of records in companies_with_sales_owners:: {df_3.count()}")
    logger.debug(f"companies with sales owners DF:\n{df_3.show(truncate=False)}")

    return df_3