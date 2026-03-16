from common_utils.spark_utils import get_spark_session
from common_utils.constants import DATABASE_NAME


# This function reads a raw CSV file from HDFS
# and stores it in the Bronze layer as a Hive table.
# Bronze layer keeps the data in raw form with minimal transformation.
def bronze_ingestion(spark, csv_path, table_name):

    # Read the CSV file from HDFS
    # header=true means the first row contains column names
    df = (
        spark.read
        .option("header", "true")
        .csv(csv_path)
    )

    # Write the raw data into a Hive table in Parquet format
    # Table name will be like bronze_customers, bronze_products, etc.
    (
        df.write
        .mode("overwrite")
        .format("parquet")
        .saveAsTable(f"{DATABASE_NAME}.bronze_{table_name}")
    )

    print(f"Created bronze_{table_name}")


# This function runs the complete Bronze layer pipeline
# for all source CSV files.
def run_bronze_layer():
    # Create Spark session with Hive support
    spark = get_spark_session()

    # Select the Hive database
    spark.sql(f"USE {DATABASE_NAME}")

    # Ingest each raw CSV file into its Bronze table
    bronze_ingestion(spark, "hdfs:///data/iphone/customers.csv", "customers")
    bronze_ingestion(spark, "hdfs:///data/iphone/products.csv", "products")
    bronze_ingestion(spark, "hdfs:///data/iphone/stores.csv", "stores")
    bronze_ingestion(spark, "hdfs:///data/iphone/sales.csv", "sales")


# This block ensures the Bronze layer runs only
# when this file is executed directly.
if __name__ == "__main__":
    run_bronze_layer()