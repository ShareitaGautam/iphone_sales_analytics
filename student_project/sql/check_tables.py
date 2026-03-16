from common_utils.spark_utils import get_spark_session
from common_utils.constants import DATABASE_NAME

# Start Spark session
spark = get_spark_session()

# Use the Hive database
spark.sql(f"USE {DATABASE_NAME}")

print("\n---- All Tables ----")
spark.sql("SHOW TABLES").show()

print("\n---- silver_sales sample ----")
spark.sql("SELECT * FROM silver_sales").show()

print("\n---- fact_sales sample ----")
spark.sql("SELECT * FROM fact_sales").show()

print("\n---- Row Counts ----")
spark.sql("SELECT COUNT(*) FROM bronze_sales").show()
spark.sql("SELECT COUNT(*) FROM silver_sales").show()
spark.sql("SELECT COUNT(*) FROM fact_sales").show()