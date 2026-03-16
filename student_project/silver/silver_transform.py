from pyspark.sql.functions import col, to_date
from common_utils.spark_utils import get_spark_session
from common_utils.constants import DATABASE_NAME


# ---------------------------------------------------
# Silver Layer – Customer Table Transformation
# This function reads raw customer data from the
# Bronze layer and applies basic cleaning.
# The cleaned data is stored in the Silver layer.
# ---------------------------------------------------
def silver_customers_transform(spark):

    # Read raw customer data from Bronze layer
    df = spark.table(f"{DATABASE_NAME}.bronze_customers")

    # Convert columns to correct data types
    clean_df = (
        df.withColumn("customer_id", col("customer_id").cast("int"))
          .withColumn("customer_name", col("customer_name").cast("string"))
          .withColumn("city", col("city").cast("string"))
          .withColumn("state", col("state").cast("string"))
    )

    # Save cleaned data to Silver layer in Parquet format
    (
        clean_df.write
        .mode("overwrite")
        .format("parquet")
        .saveAsTable(f"{DATABASE_NAME}.silver_customers")
    )

    print("Created silver_customers")


# ---------------------------------------------------
# Silver Layer – Product Table Transformation
# Cleans product data and fixes column data types.
# ---------------------------------------------------
def silver_products_transform(spark):

    # Read raw product data from Bronze layer
    df = spark.table(f"{DATABASE_NAME}.bronze_products")

    # Convert columns to correct data types
    clean_df = (
        df.withColumn("product_id", col("product_id").cast("int"))
          .withColumn("product_name", col("product_name").cast("string"))
          .withColumn("category", col("category").cast("string"))
          .withColumn("unit_price", col("unit_price").cast("int"))
    )

    # Save cleaned product data to Silver layer
    (
        clean_df.write
        .mode("overwrite")
        .format("parquet")
        .saveAsTable(f"{DATABASE_NAME}.silver_products")
    )

    print("Created silver_products")


# ---------------------------------------------------
# Silver Layer – Store Table Transformation
# Cleans store data and standardizes column types.
# ---------------------------------------------------
def silver_stores_transform(spark):

    # Read raw store data from Bronze layer
    df = spark.table(f"{DATABASE_NAME}.bronze_stores")

    # Convert columns to appropriate data types
    clean_df = (
        df.withColumn("store_id", col("store_id").cast("int"))
          .withColumn("store_name", col("store_name").cast("string"))
          .withColumn("city", col("city").cast("string"))
          .withColumn("state", col("state").cast("string"))
    )

    # Save cleaned store data into Silver layer
    (
        clean_df.write
        .mode("overwrite")
        .format("parquet")
        .saveAsTable(f"{DATABASE_NAME}.silver_stores")
    )

    print("Created silver_stores")


# ---------------------------------------------------
# Silver Layer – Sales Table Transformation
# This function cleans the sales transaction data.
# It also converts the sale_date column to Date type
# and partitions the table by date for faster queries.
# ---------------------------------------------------
def silver_sales_transform(spark):

    # Read raw sales data from Bronze layer
    df = spark.table(f"{DATABASE_NAME}.bronze_sales")

    # Clean and convert column data types
    clean_df = (
        df.withColumn("sale_id", col("sale_id").cast("int"))
          .withColumn("product_id", col("product_id").cast("int"))
          .withColumn("customer_id", col("customer_id").cast("int"))
          .withColumn("store_id", col("store_id").cast("int"))
          .withColumn("quantity", col("quantity").cast("int"))

          # Convert sale_date from string to Date type
          .withColumn("sale_date", to_date(col("sale_date")))
    )

    # Save cleaned sales data to Silver layer
    # Partition by sale_date to improve query performance
    (
        clean_df.write
        .mode("overwrite")
        .partitionBy("sale_date")
        .format("parquet")
        .saveAsTable(f"{DATABASE_NAME}.silver_sales")
    )

    print("Created silver_sales")


# ---------------------------------------------------
# Run the complete Silver layer pipeline
# This function executes all transformation steps
# for customers, products, stores, and sales tables.
# ---------------------------------------------------
def run_silver_layer():

    # Start Spark session
    spark = get_spark_session()

    # Use the correct Hive database
    spark.sql(f"USE {DATABASE_NAME}")

    # Run all Silver layer transformations
    silver_customers_transform(spark)
    silver_products_transform(spark)
    silver_stores_transform(spark)
    silver_sales_transform(spark)


# ---------------------------------------------------
# Ensures the Silver layer runs only when this script
# is executed directly.
# ---------------------------------------------------
if __name__ == "__main__":
    run_silver_layer()