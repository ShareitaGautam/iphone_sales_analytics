from pyspark.sql import SparkSession


def get_spark_session(app_name="iphone_sales_analytics"):
    spark = (
        SparkSession.builder
        .appName(app_name)
        .enableHiveSupport()
        .getOrCreate()
    )
    return spark