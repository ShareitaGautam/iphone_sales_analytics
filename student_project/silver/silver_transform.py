from pyspark.sql.functions import col, to_date
from common_utils.spark_utils import get_spark_session
from common_utils.constants import DATABASE_NAME


def silver_customers_transform(spark):
    df = spark.table(f"{DATABASE_NAME}.bronze_customers")

    clean_df = (
        df.withColumn("customer_id", col("customer_id").cast("int"))
          .withColumn("customer_name", col("customer_name").cast("string"))
          .withColumn("city", col("city").cast("string"))
          .withColumn("state", col("state").cast("string"))
    )

    (
        clean_df.write
        .mode("overwrite")
        .format("parquet")
        .saveAsTable(f"{DATABASE_NAME}.silver_customers")
    )

    print("Created silver_customers")


def silver_products_transform(spark):
    df = spark.table(f"{DATABASE_NAME}.bronze_products")

    clean_df = (
        df.withColumn("product_id", col("product_id").cast("int"))
          .withColumn("product_name", col("product_name").cast("string"))
          .withColumn("category", col("category").cast("string"))
          .withColumn("unit_price", col("unit_price").cast("int"))
    )

    (
        clean_df.write
        .mode("overwrite")
        .format("parquet")
        .saveAsTable(f"{DATABASE_NAME}.silver_products")
    )

    print("Created silver_products")


def silver_stores_transform(spark):
    df = spark.table(f"{DATABASE_NAME}.bronze_stores")

    clean_df = (
        df.withColumn("store_id", col("store_id").cast("int"))
          .withColumn("store_name", col("store_name").cast("string"))
          .withColumn("city", col("city").cast("string"))
          .withColumn("state", col("state").cast("string"))
    )

    (
        clean_df.write
        .mode("overwrite")
        .format("parquet")
        .saveAsTable(f"{DATABASE_NAME}.silver_stores")
    )

    print("Created silver_stores")


def silver_sales_transform(spark):
    df = spark.table(f"{DATABASE_NAME}.bronze_sales")

    clean_df = (
        df.withColumn("sale_id", col("sale_id").cast("int"))
          .withColumn("product_id", col("product_id").cast("int"))
          .withColumn("customer_id", col("customer_id").cast("int"))
          .withColumn("store_id", col("store_id").cast("int"))
          .withColumn("quantity", col("quantity").cast("int"))
          .withColumn("sale_date", to_date(col("sale_date")))
    )

    (
        clean_df.write
        .mode("overwrite")
        .partitionBy("sale_date")
        .format("parquet")
        .saveAsTable(f"{DATABASE_NAME}.silver_sales")
    )

    print("Created silver_sales")


def run_silver_layer():
    spark = get_spark_session()
    spark.sql(f"USE {DATABASE_NAME}")

    silver_customers_transform(spark)
    silver_products_transform(spark)
    silver_stores_transform(spark)
    silver_sales_transform(spark)


if __name__ == "__main__":
    run_silver_layer()