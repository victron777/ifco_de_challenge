import streamlit as st
from pyspark.sql import SparkSession
from src.analysis import (
    calculate_crate_distribution,
    calculate_orders_with_contact,
    calculate_orders_contact_address,
    calculate_sales_commissions,
    calculate_companies_with_sales_owners,
    calculate_sales_owner_training,
    load_data
)
import matplotlib.pyplot as plt
import src.config as config
import os

"""
Create the Spark Session
"""
# Set Spark local IP
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

spark = SparkSession.builder \
    .master("local[*]") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .appName("Streamlit Dashboard") \
    .getOrCreate()

# Load data
orders_df, invoicing_df = load_data(spark,  config.ORDERS_FILE,  config.INVOICING_FILE)

def main():
    st.title("Exploratory Data Analysis Dashboard")

    # Sidebar for user inputs
    st.sidebar.header("Options")
    analysis_option = st.sidebar.selectbox(
        "Choose Analysis",
        ["Crate Distribution", "Orders with Contact", "Orders with Contact Address", "Sales Commissions", "Companies with Sales Owners", "Sales owners who need training"]
    )

    # Display analysis based on user selection
    if analysis_option == "Crate Distribution":
        st.header("Crate Distribution per Company")
        crate_distribution_df = calculate_crate_distribution(spark, orders_df)
        st.write(crate_distribution_df.toPandas())

        # Plot crate distribution
        st.subheader("Crate Distribution Chart")
        plt.figure(figsize=(10, 6))
        crate_distribution_pd = crate_distribution_df.toPandas()
        plt.bar(crate_distribution_pd["company_name"], crate_distribution_pd["order_count"])
        plt.xlabel("Company Name")
        plt.ylabel("Order Count")
        plt.xticks(rotation=45)
        st.pyplot(plt)

    elif analysis_option == "Orders with Contact":
        st.header("Orders with Contact Information")
        orders_contact_df = calculate_orders_with_contact(orders_df)
        st.write(orders_contact_df.toPandas())

    elif analysis_option == "Orders with Contact Address":
        st.header("Orders with Contact Address Information")
        orders_contact_df = calculate_orders_contact_address(orders_df)
        st.write(orders_contact_df.toPandas())

    elif analysis_option == "Sales Commissions":
        st.header("Sales Team Commissions")
        sales_commissions_df = calculate_sales_commissions(orders_df, invoicing_df)
        st.write(sales_commissions_df.toPandas())

    elif analysis_option == "Companies with Sales Owners":
        st.header("Companies with Sales Owners")
        companies_sales_owners_df = calculate_companies_with_sales_owners(orders_df)
        st.write(companies_sales_owners_df.toPandas())

    elif analysis_option == "Sales owners who need training":
        st.header("Sales owners who need training")
        sales_owner_training_df = calculate_sales_owner_training(orders_df)
        st.write(sales_owner_training_df.toPandas())

if __name__ == "__main__":
    main()