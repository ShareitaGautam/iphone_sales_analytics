from pyspark.sql.functions import year, month, dayofmonth, col
from common_utils.spark_utils import get_spark_session
from common_utils.constants import DATABASE_NAME


# ---------------------------------------------------
# Create Dimension Table: Customer
# This table stores customer details used for analysis.
# Data is taken from the cleaned Silver layer.
# ---------------------------------------------------
def load_dim_customer(spark):

    # Read cleaned customer data from the Silver layer
    df = spark.table(f"{DATABASE_NAME}.silver_customers")

    # Select required columns for the dimension table
    dim_df = df.select(
        "customer_id",
        "customer_name",
        "city",
        "state"
    )

    # Save the dimension table in Hive using Parquet format
    (
        dim_df.write
        .mode("overwrite")
        .format("parquet")
        .saveAsTable(f"{DATABASE_NAME}.dim_customer")
    )

    print("Created dim_customer")


# ---------------------------------------------------
# Create Dimension Table: Product
# This table contains product details such as name,
# category, and price.
# ---------------------------------------------------
def load_dim_product(spark):

    # Read product data from Silver layer
    df = spark.table(f"{DATABASE_NAME}.silver_products")

    # Select columns required for the product dimension
    dim_df = df.select(
        "product_id",
        "product_name",
        "category",
        "unit_price"
    )

    # Save the table in Hive
    (
        dim_df.write
        .mode("overwrite")
        .format("parquet")
        .saveAsTable(f"{DATABASE_NAME}.dim_product")
    )

    print("Created dim_product")


# ---------------------------------------------------
# Create Dimension Table: Store
# This table contains store information such as
# store name, city, and state.
# ---------------------------------------------------
def load_dim_store(spark):

    # Read store data from Silver layer
    df = spark.table(f"{DATABASE_NAME}.silver_stores")

    # Select relevant columns
    dim_df = df.select(
        "store_id",
        "store_name",
        "city",
        "state"
    )

    # Write dimension table to Hive
    (
        dim_df.write
        .mode("overwrite")
        .format("parquet")
        .saveAsTable(f"{DATABASE_NAME}.dim_store")
    )

    print("Created dim_store")


# ---------------------------------------------------
# Create Dimension Table: Date
# This table is generated from sales dates and
# contains year, month, and day columns for
# time-based analytics.
# ---------------------------------------------------
def load_dim_date(spark):

    # Read sales data from Silver layer
    df = spark.table(f"{DATABASE_NAME}.silver_sales")

    # Extract unique sale dates and derive date components
    dim_df = (
        df.select(col("sale_date").alias("date_key"))
          .distinct()
          .withColumn("year", year(col("date_key")))
          .withColumn("month", month(col("date_key")))
          .withColumn("day", dayofmonth(col("date_key")))
    )

    # Save date dimension table
    (
        dim_df.write
        .mode("overwrite")
        .format("parquet")
        .saveAsTable(f"{DATABASE_NAME}.dim_date")
    )

    print("Created dim_date")


# ---------------------------------------------------
# Create Fact Table: fact_sales
# This table stores the main sales transactions.
# It links all dimension tables and contains
# measures used for analytics.
#
# Reference Fact Table Schema:
#
# CREATE TABLE fact_sales (
#   sale_id INT,
#   customer_id INT,
#   product_id INT,
#   store_id INT,
#   date_key DATE,
#   quantity INT,
#   total_amount INT   -- total_amount = quantity * unit_price
# )
# PARTITIONED BY (date_key)
# STORED AS PARQUET;
# ---------------------------------------------------
def load_fact_sales(spark):

    # Read cleaned sales data from Silver layer
    sales = spark.table(f"{DATABASE_NAME}.silver_sales")

    # Read product table to get unit_price
    products = spark.table(f"{DATABASE_NAME}.silver_products")

    # Join sales with product data so revenue can be calculated
    fact_df = (
        sales.join(products, "product_id")

        # Calculate total sales amount
        .withColumn("total_amount", col("quantity") * col("unit_price"))

        # Select columns needed for the fact table
        .select(
            "sale_id",
            "customer_id",
            "product_id",
            "store_id",
            col("sale_date").alias("date_key"),
            "quantity",
            "total_amount"
        )
    )

    # Write fact table to Hive
    # Partition by date_key to improve query performance
    (
        fact_df.write
        .mode("overwrite")
        .partitionBy("date_key")
        .format("parquet")
        .saveAsTable(f"{DATABASE_NAME}.fact_sales")
    )

    print("Created fact_sales")


# ---------------------------------------------------
# Run the complete Gold layer pipeline
# This creates all dimension tables and the fact table.
# ---------------------------------------------------
def run_gold_dimensions():

    # Start Spark session
    spark = get_spark_session()

    # Use the correct Hive database
    spark.sql(f"USE {DATABASE_NAME}")

    # Create dimension tables
    load_dim_customer(spark)
    load_dim_product(spark)
    load_dim_store(spark)
    load_dim_date(spark)

    # Create fact table
    load_fact_sales(spark)


# ---------------------------------------------------
# This ensures the script runs only when executed
# directly and not when imported as a module.
# ---------------------------------------------------
if __name__ == "__main__":
    run_gold_dimensions()