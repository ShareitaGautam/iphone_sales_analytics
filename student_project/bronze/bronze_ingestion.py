from common_utils.spark_utils import get_spark_session
from common_utils.constants import DATABASE_NAME


def bronze_ingestion(spark, csv_path, table_name):

    df = (
        spark.read
        .option("header", "true")
        .csv(csv_path)
    )

    (
        df.write
        .mode("overwrite")
        .format("parquet")
        .saveAsTable(f"{DATABASE_NAME}.bronze_{table_name}")
    )

    print(f"Created bronze_{table_name}")


def run_bronze_layer():
    spark = get_spark_session()

    spark.sql(f"USE {DATABASE_NAME}")

    bronze_ingestion(spark, "hdfs:///data/iphone/customers.csv", "customers")
    bronze_ingestion(spark, "hdfs:///data/iphone/products.csv", "products")
    bronze_ingestion(spark, "hdfs:///data/iphone/stores.csv", "stores")
    bronze_ingestion(spark, "hdfs:///data/iphone/sales.csv", "sales")


if __name__ == "__main__":
    run_bronze_layer()