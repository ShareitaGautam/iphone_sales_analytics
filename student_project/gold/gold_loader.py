from pyspark.sql.functions import year, month, dayofmonth, col
from common_utils.spark_utils import get_spark_session
from common_utils.constants import DATABASE_NAME


def load_dim_customer(spark):
    df = spark.table(f"{DATABASE_NAME}.silver_customers")

    dim_df = df.select(
        "customer_id",
        "customer_name",
        "city",
        "state"
    )

    (
        dim_df.write
        .mode("overwrite")
        .format("parquet")
        .saveAsTable(f"{DATABASE_NAME}.dim_customer")
    )

    print("Created dim_customer")


def load_dim_product(spark):
    df = spark.table(f"{DATABASE_NAME}.silver_products")

    dim_df = df.select(
        "product_id",
        "product_name",
        "category",
        "unit_price"
    )

    (
        dim_df.write
        .mode("overwrite")
        .format("parquet")
        .saveAsTable(f"{DATABASE_NAME}.dim_product")
    )

    print("Created dim_product")


def load_dim_store(spark):
    df = spark.table(f"{DATABASE_NAME}.silver_stores")

    dim_df = df.select(
        "store_id",
        "store_name",
        "city",
        "state"
    )

    (
        dim_df.write
        .mode("overwrite")
        .format("parquet")
        .saveAsTable(f"{DATABASE_NAME}.dim_store")
    )

    print("Created dim_store")


def load_dim_date(spark):
    df = spark.table(f"{DATABASE_NAME}.silver_sales")

    dim_df = (
        df.select(col("sale_date").alias("date_key"))
          .distinct()
          .withColumn("year", year(col("date_key")))
          .withColumn("month", month(col("date_key")))
          .withColumn("day", dayofmonth(col("date_key")))
    )

    (
        dim_df.write
        .mode("overwrite")
        .format("parquet")
        .saveAsTable(f"{DATABASE_NAME}.dim_date")
    )

    print("Created dim_date")

def load_fact_sales(spark):
    sales = spark.table(f"{DATABASE_NAME}.silver_sales")
    products = spark.table(f"{DATABASE_NAME}.silver_products")

    fact_df = (
        sales.join(products, "product_id")
             .withColumn("total_amount", col("quantity") * col("unit_price"))
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

    (
        fact_df.write
        .mode("overwrite")
        .partitionBy("date_key")
        .format("parquet")
        .saveAsTable(f"{DATABASE_NAME}.fact_sales")
    )

    print("Created fact_sales")


def run_gold_dimensions():
    spark = get_spark_session()
    spark.sql(f"USE {DATABASE_NAME}")

    load_dim_customer(spark)
    load_dim_product(spark)
    load_dim_store(spark)
    load_dim_date(spark)
    load_fact_sales(spark)



if __name__ == "__main__":
    run_gold_dimensions()